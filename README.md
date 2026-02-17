# Snowflake データ操作アプリ（Streamlit in Snowflake）

簡単な GUI 操作で Snowflake 上のデータを探索・取込できる Streamlit in Snowflake アプリケーションです。

## 機能一覧

| ページ | 概要 |
|--------|------|
| 🏠 ホーム | ダッシュボード・お知らせ表示 |
| 🔍 定型検索 | 事前定義した検索テンプレートの作成・実行 |
| 📊 非定型検索 | テーブル結合（最大3テーブル）やフィルター条件を GUI で自由に設定 |
| 🤖 自然言語分析 | Cortex Analyst による自然言語データ分析 ※現在準備中 |
| 📥 データ取込 | CSV ファイルアップロードから Snowflake テーブルへの取込 |
| 🔧 保守・運用 | お知らせ管理・テーブル情報確認 |

## セットアップ手順

### 1. 事前準備

- Snowflake アカウント
- `ACCOUNTADMIN` または必要な権限を持つロール
- Git 連携用の API Integration（`git_api_integration`）

### 2. SQL セットアップ

`simple_search_app/setup.sql` を Snowflake ワークシートで実行してください。

```sql
-- 主な処理内容:
-- 1. application_db / bank_db データベース作成
-- 2. バックエンドテーブル作成（定型検索・非定型検索・お知らせ）
-- 3. デモ用データ投入（顧客50件・取引150件）
-- 4. Streamlit in Snowflake アプリのデプロイ
-- 5. セマンティックビュー作成
```

### 3. アプリのデプロイ

setup.sql 内の Git 連携部分を環境に合わせて修正し、実行します。

```sql
-- Git連携（API Integrationは事前作成が必要）
CREATE OR REPLACE GIT REPOSITORY GIT_INTEGRATION_FOR_HANDSON
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/kmotokubota/simple_serach_streamlit_app.git';

-- Streamlit in Snowflake アプリ作成
CREATE OR REPLACE STREAMLIT simple_search_app
    FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/simple_search_app
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = COMPUTE_WH;
```

## ディレクトリ構成

```
simple_search_app/
├── environment.yml        # 依存パッケージ定義
├── setup.sql              # 環境構築・デモデータ・デプロイ用SQL
├── streamlit_app.py       # メインページ（ホーム）
└── pages/
    ├── 1_standard_search.py   # 定型検索
    ├── 2_adhoc_search.py      # 非定型検索
    ├── 3_cortex_analyst.py    # 自然言語分析（準備中）
    ├── 4_ingest.py            # データ取込
    └── 5_admin.py             # 保守・運用
```

## 備考

- 自然言語分析（Cortex Analyst）およびセマンティックビュー関連の機能は現在一時的に無効化されています
- 有効化する場合は `setup.sql` のセマンティックビュー作成を実行し、`pages/3_cortex_analyst.py` を更新してください
