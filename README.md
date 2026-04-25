# SNS運用ダッシュボード

Lumina (@lumina_ai_art) / ミャクやん (@neet_myaku) のSNS運用管理ダッシュボード。

## 機能

- 📊 アカウント別KPI（フォロワー・インプレ・エンゲージメント）
- 🔥 バズ投稿ストック管理（X検索URL生成・編集・削除）
- 📅 定期投稿テンプレ管理
- #️⃣ ハッシュタグ効果測定
- 🏆 競合アカウント比較

## ローカル起動

```bash
pip install -r requirements.txt
streamlit run app.py
```

`.env`ファイルが必要（`.streamlit/secrets.toml.example` 参照）。

## デプロイ

[Streamlit Cloud](https://share.streamlit.io/) に接続済み。
Secrets管理は `.streamlit/secrets.toml.example` 参照。

## DB

Supabase PostgreSQL（東京リージョン）。
スキーマは `marketing.*`。
