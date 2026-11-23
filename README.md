# 応援ランキング＆スパイク負荷検証API

イベントやライブ配信時の同時応援リクエスト集中（スパイク）を想定し、処理方式の違いによる負荷・応答速度の変化を検証するためのGo (Gin) APIと計測ダッシュボードです。

## 技術スタック
- **Backend:** Go, Gin
- **Frontend / Dashboard:** HTML / CSS / JavaScript (TypeScriptベースで拡張可能、`localStorage` 履歴保存・Markdown表コピー対応)
- **Container:** Docker, Docker Compose

## 起動方法

```bash
docker compose up --build -d
```

起動後、ブラウザから各モードのダッシュボードにアクセスできます。
- `optimized` モード: http://localhost:8080/
- `naive` モード: http://localhost:8081/

停止する場合は以下を実行します。
```bash
docker compose down
```

## 設計意図・工夫した点
- **`optimized` と `naive` の並行起動:** ポートを分けて起動できるようにし、同一条件での挙動・速度比較をしやすくしました。
- **スパイク負荷の可視化:** ブラウザ上からリクエスト数・同時実行数を指定して負荷テストを実行でき、計測履歴を `localStorage` に保持しつつ Markdown 表形式でコピーできるようにしました。

## 負荷テスト比較結果
※計測時の条件（対象・リクエスト数・同時実行数・実行環境）を明記します。

| 日時 | モード | リクエスト数 | 所要時間(ms) | スループット(req/s) |
| :--- | :--- | ---: | ---: | ---: |
| 11:29:44 | optimized | 200 | 295 | 678.0 |
| - | naive | 200 | （計測値を記入） | （計測値を記入） |

## 今後の拡張案
- DB（PostgreSQL / SQLite）やキャッシュ（Redis）コンテナの追加
- レート制限の組み込みと、より高負荷な条件でのボトルネック検証