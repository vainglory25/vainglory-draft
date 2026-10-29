# VainGlory ドラフト (Vercel + Upstash Redis)

## フォルダ構成
```
index.html
package.json
api/room.js
images/hero-captain.png  hero-jungler.png  hero-laner-1.png  hero-laner-2.png
```

## デプロイ手順
1. 上のフォルダ一式を GitHub などに置き、Vercel でプロジェクトとして取り込む(または `vercel` コマンドでデプロイ)。
2. Vercel のプロジェクト画面 → **Storage**(または Marketplace)→ **Upstash Redis** を作成して、このプロジェクトに接続する。
   - 環境変数 `KV_REST_API_URL` / `KV_REST_API_TOKEN`(または `UPSTASH_REDIS_REST_URL` / `UPSTASH_REDIS_REST_TOKEN`)が自動で入ります。コードの変更は不要です。
3. 環境変数を入れたあとに **再デプロイ** する(これをしないと「データベースが設定されていません」と出ます)。
4. 別々のネットワークの2台で、ルーム作成 → ID入室 → ドラフトを試す。

## しくみ
- 進行・検証はすべて `api/room.js`(サーバー側)。ブラウザは約1.3秒ごとに状態を取得します。
- 仮ピックの候補は、同じチームの人にだけ返します(相手・観戦者には送られません)。
- ルームは最後の更新から6時間で自動的に消えます。
