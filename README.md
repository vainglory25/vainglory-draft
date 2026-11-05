# VainGlory ドラフト (Vercel + Upstash Redis)

## フォルダ構成
```
index.html          ホストログイン版(通常)
free.html           無料版(ログイン不要・部屋作成は同じ回線から24時間に、部屋の作成3回まで・ドラフトの開始3回まで)
                    ※代表者モードは、部屋の作成画面の「代表者モード」チェックで選びます(leader.html / leader-free.html は廃止)
package.json
vercel.json
api/room.js
images/hero-captain.png  hero-jungler.png  hero-laner-1.png  hero-laner-2.png
```

## デプロイ手順
1. 上のフォルダ一式を GitHub などに置き、Vercel でプロジェクトとして取り込む(または `vercel` コマンドでデプロイ)。
2. Vercel のプロジェクト画面 → **Storage**(または Marketplace)→ **Upstash Redis** を作成して、このプロジェクトに接続する。
   - 環境変数 `KV_REST_API_URL` / `KV_REST_API_TOKEN`(または `UPSTASH_REDIS_REST_URL` / `UPSTASH_REDIS_REST_TOKEN`)が自動で入ります。コードの変更は不要です。
3. 環境変数を入れたあとに **再デプロイ** する(これをしないと「データベースが設定されていません」と出ます)。
4. 別々のネットワークの2台で、ルーム作成 → 招待リンクで入室 → ドラフトを試す。

## ホストログイン(ルームを作れるのはホストだけ)
Vercel のプロジェクト → Settings → Environment Variables に次の2つを追加し、再デプロイします。
- `HOST_USER` : ホストのユーザー名(好きな文字列)
- `HOST_PASSWORD` : ホストのパスワード(長めで推測されにくいもの)
- `HOST_USERS` : (任意)ホストを複数にしたいとき。`ユーザー名:パスワード` をカンマでつなげます。例: `taro:pass1,hanako:pass2`(パスワードにカンマは使えません)。`HOST_USER`/`HOST_PASSWORD` と併用でき、どれか1組が合えばログインできます。追加・削除は、環境変数を書き換えて再デプロイします

URL ごとの動き:
- `https://…vercel.app` : ホストログインだけ表示。ログインするとルーム作成と入室が出る。
- `…?room=ID` : 入室画面だけ(ロビーでホーム/アウェイに参加できる)。
- `…?room=ID&team=home|away` : 入室ボタンを押すとそのチームに着席(未準備)。
- `…?room=ID&watch=1` : 観戦で入室。
パスワードの照合はサーバー側で行い、失敗が同じIPから10分に10回続くと一時的に拒否します。

## しくみ
- 進行・検証はすべて `api/room.js`(サーバー側)。ブラウザは約1.3秒ごとに状態を取得します。
- 仮ピックの候補は、同じチームの人にだけ返します(相手・観戦者には送られません)。
- ルームは最後の更新から6時間で自動的に消えます。
