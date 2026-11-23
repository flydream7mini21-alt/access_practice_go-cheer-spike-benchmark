package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

const rankingCacheKey = "cheer:ranking:v1"

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

type RankItem struct {
	TargetID string `json:"target_id"`
	Score    int64  `json:"score"`
}

// PostgreSQL を使った永続化ストア
type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore() (*PostgresStore, error) {
	host := getEnv("DB_HOST", "localhost")
	port := getEnv("DB_PORT", "5432")
	user := getEnv("DB_USER", "cheer_user")
	password := getEnv("DB_PASSWORD", "cheer_pass")
	dbName := getEnv("DB_NAME", "cheer_db")

	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbName,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	var pingErr error
	for i := 0; i < 15; i++ {
		pingErr = db.Ping()
		if pingErr == nil {
			break
		}
		log.Printf("PostgreSQL 接続待ち (%d/15): %v", i+1, pingErr)
		time.Sleep(1 * time.Second)
	}
	if pingErr != nil {
		_ = db.Close()
		return nil, pingErr
	}

	store := &PostgresStore{db: db}
	if err := store.ensureSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (s *PostgresStore) ensureSchema() error {
	createSQL := `
	CREATE TABLE IF NOT EXISTS cheer_scores (
		target_id TEXT PRIMARY KEY,
		score BIGINT NOT NULL DEFAULT 0
	);`
	if _, err := s.db.Exec(createSQL); err != nil {
		return fmt.Errorf("cheer_scores テーブル作成失敗: %w", err)
	}

	seedSQL := `
	INSERT INTO cheer_scores (target_id, score)
	VALUES ('talent_a', 0), ('talent_b', 0), ('talent_c', 0)
	ON CONFLICT (target_id) DO NOTHING;`
	if _, err := s.db.Exec(seedSQL); err != nil {
		return fmt.Errorf("初期データ投入失敗: %w", err)
	}

	return nil
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}

func (s *PostgresStore) AddScore(targetID string, delta int64) error {
	query := `
	INSERT INTO cheer_scores (target_id, score)
	VALUES ($1, $2)
	ON CONFLICT (target_id)
	DO UPDATE SET score = cheer_scores.score + EXCLUDED.score;`
	_, err := s.db.Exec(query, targetID, delta)
	return err
}

func (s *PostgresStore) GetRanking() ([]RankItem, error) {
	rows, err := s.db.Query(`SELECT target_id, score FROM cheer_scores`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make([]RankItem, 0, 8)
	for rows.Next() {
		var item RankItem
		if err := rows.Scan(&item.TargetID, &item.Score); err != nil {
			return nil, err
		}
		list = append(list, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].Score == list[j].Score {
			return list[i].TargetID < list[j].TargetID
		}
		return list[i].Score > list[j].Score
	})
	return list, nil
}

func initRedisClient() (*redis.Client, error) {
	addr := getEnv("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var pingErr error
	for i := 0; i < 15; i++ {
		pingErr = rdb.Ping(ctx).Err()
		if pingErr == nil {
			break
		}
		log.Printf("Redis 接続待ち (%d/15): %v", i+1, pingErr)
		time.Sleep(1 * time.Second)
	}
	if pingErr != nil {
		_ = rdb.Close()
		return nil, pingErr
	}

	return rdb, nil
}

// スパイク対策用のバッファ＆Redisキャッシュ管理
type CheerService struct {
	store    *PostgresStore
	rdb      *redis.Client
	mode     string // "naive" or "optimized"
	bufferMu sync.Mutex
	buffer   map[string]int64
	cacheTTL time.Duration
}

func NewCheerService(store *PostgresStore, rdb *redis.Client, mode string) *CheerService {
	return &CheerService{
		store:    store,
		rdb:      rdb,
		mode:     mode,
		buffer:   make(map[string]int64),
		cacheTTL: 1 * time.Second,
	}
}

// 応援リクエスト処理
func (s *CheerService) Cheer(targetID string) error {
	if s.mode == "naive" {
		// 対策前：リクエストごとに毎回PostgreSQLへ直接書き込む
		return s.store.AddScore(targetID, 1)
	}

	// 対策後：メモリ上のバッファに積むだけ
	s.bufferMu.Lock()
	s.buffer[targetID]++
	s.bufferMu.Unlock()
	return nil
}

// 定期的にバッファをまとめてPostgreSQLへ書き込むワーカー
func (s *CheerService) StartFlusher(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.flush() // 終了時に未反映分を必ず書き出す
			return
		case <-ticker.C:
			s.flush()
		}
	}
}

func (s *CheerService) flush() {
	s.bufferMu.Lock()
	if len(s.buffer) == 0 {
		s.bufferMu.Unlock()
		return
	}
	batch := s.buffer
	s.buffer = make(map[string]int64)
	s.bufferMu.Unlock()

	for id, delta := range batch {
		if err := s.store.AddScore(id, delta); err != nil {
			log.Printf("flush error (target=%s, delta=%d): %v", id, delta, err)
		}
	}
}

// ランキング取得（optimizedモード時はRedisキャッシュを利用）
func (s *CheerService) GetRanking(ctx context.Context) ([]RankItem, error) {
	if s.mode == "naive" {
		return s.store.GetRanking()
	}

	if s.rdb != nil {
		cached, err := s.rdb.Get(ctx, rankingCacheKey).Result()
		if err == nil && cached != "" {
			var list []RankItem
			if unmarshalErr := json.Unmarshal([]byte(cached), &list); unmarshalErr == nil {
				return list, nil
			}
		}
	}

	fresh, err := s.store.GetRanking()
	if err != nil {
		return nil, err
	}

	if s.rdb != nil {
		if payload, marshalErr := json.Marshal(fresh); marshalErr == nil {
			_ = s.rdb.Set(ctx, rankingCacheKey, payload, s.cacheTTL).Err()
		}
	}

	return fresh, nil
}

func main() {
	mode := os.Getenv("MODE")
	if mode == "" {
		mode = "optimized"
	}

	store, err := NewPostgresStore()
	if err != nil {
		log.Fatalf("PostgreSQL 初期化失敗: %v", err)
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			log.Printf("PostgreSQL クローズエラー: %v", closeErr)
		}
	}()

	rdb, err := initRedisClient()
	if err != nil {
		log.Fatalf("Redis 初期化失敗: %v", err)
	}
	defer func() {
		if closeErr := rdb.Close(); closeErr != nil {
			log.Printf("Redis クローズエラー: %v", closeErr)
		}
	}()

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	svc := NewCheerService(store, rdb, mode)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if mode == "optimized" {
		go svc.StartFlusher(ctx, 500*time.Millisecond)
	}

	// 応援API
	r.POST("/cheer/:target_id", func(c *gin.Context) {
		targetID := c.Param("target_id")
		if err := svc.Cheer(targetID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"status": "error",
				"error":  err.Error(),
			})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{
			"status": "accepted",
			"target": targetID,
			"mode":   mode,
		})
	})

	// ダッシュボード画面（html/index.html）を配信
	r.GET("/", func(c *gin.Context) {
		c.File("html/index.html")
	})

	// ランキングAPI
	r.GET("/ranking", func(c *gin.Context) {
		ranking, err := svc.GetRanking(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"status": "error",
				"error":  err.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"mode":    mode,
			"ranking": ranking,
		})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("Server started on :%s (MODE=%s)", port, mode)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down gracefully...")

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
	log.Println("Server exited")
}
