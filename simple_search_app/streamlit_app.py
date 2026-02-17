# =========================================================
# Snowflakeデータ操作アプリケーション
# Streamlit in Snowflake対応版
# =========================================================
# Created by kdaigo
# 最終更新: 2025/09/24
# =========================================================

# =========================================================
# 必要なライブラリのインポート
# =========================================================
import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, call_function, when_matched, when_not_matched

# =========================================================
# ページ設定とセッション初期化
# =========================================================
st.set_page_config(
    layout="wide",
    initial_sidebar_state="expanded",
    page_title="🏠 ホーム",
    page_icon="🏠"
)

# Snowflakeセッションの取得
@st.cache_resource
def get_snowflake_session():
    """Snowflakeセッションを取得"""
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# セッション状態の初期化
# =========================================================
if 'recent_searches' not in st.session_state:
    st.session_state.recent_searches = []

if 'favorites' not in st.session_state:
    st.session_state.favorites = []

# DB/スキーマ選択のセッション状態
if 'selected_database' not in st.session_state:
    st.session_state.selected_database = ""
if 'selected_schema' not in st.session_state:
    st.session_state.selected_schema = ""

# お知らせはデータベーステーブルで管理（setup SQLで事前作成済み）

# ユーザーオプション設定を削除（シンプル化のため）

# =========================================================
# 共通関数：DB/スキーマ動的選択
# =========================================================
@st.cache_data(ttl=60, show_spinner=False)
def get_available_databases():
    """アクセス可能なデータベース一覧を取得"""
    try:
        result = session.sql("SHOW DATABASES").collect()
        # システムデータベースを除外
        excluded_dbs = {'SNOWFLAKE', 'SNOWFLAKE_SAMPLE_DATA'}
        return sorted([row['name'] for row in result if row['name'] not in excluded_dbs])
    except Exception as e:
        st.error(f"データベース取得エラー: {str(e)}")
        return []

@st.cache_data(ttl=60, show_spinner=False)
def get_available_schemas(database_name: str):
    """指定DBのスキーマ一覧を取得"""
    if not database_name:
        return []
    try:
        result = session.sql(f"SHOW SCHEMAS IN DATABASE {database_name}").collect()
        # システムスキーマを除外
        excluded_schemas = {'INFORMATION_SCHEMA'}
        return sorted([row['name'] for row in result if row['name'] not in excluded_schemas])
    except Exception as e:
        st.error(f"スキーマ取得エラー: {str(e)}")
        return []

@st.cache_data(ttl=60, show_spinner=False)
def get_available_tables_dynamic(database: str, schema: str):
    """指定スキーマのテーブル一覧を取得"""
    if not database or not schema:
        return []
    try:
        result = session.sql(f"SHOW TABLES IN {database}.{schema}").collect()
        # システムテーブルを除外
        excluded_tables = {'STANDARD_SEARCH_OBJECTS', 'ADHOC_SEARCH_OBJECTS', 'ANNOUNCEMENTS'}
        excluded_prefixes = ('SNOWPARK_TEMP_TABLE_',)
        tables = []
        for row in result:
            name = row['name']
            if name not in excluded_tables and not name.upper().startswith(excluded_prefixes):
                tables.append(name)
        return sorted(tables)
    except Exception as e:
        # エラーは静かに処理（権限がない場合など）
        return []

@st.cache_data(ttl=60, show_spinner=False)
def get_available_views_dynamic(database: str, schema: str):
    """指定スキーマのビュー一覧を取得"""
    if not database or not schema:
        return []
    try:
        result = session.sql(f"SHOW VIEWS IN {database}.{schema}").collect()
        return sorted([row['name'] for row in result])
    except:
        return []

# =========================================================
# 共通関数
# =========================================================
# check_table_exists関数は削除 - 使用されていない

def get_table_count(table_name: str) -> int:
    """テーブルのレコード数を取得"""
    try:
        result = session.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()
        return result[0]['COUNT']
    except:
        return 0

def add_recent_search(search_type: str, search_name: str, status: str = "完了"):
    """最近の検索履歴に追加"""
    search_record = {
        'timestamp': datetime.now(),
        'type': search_type,
        'name': search_name,
        'status': status
    }
    st.session_state.recent_searches.insert(0, search_record)
    # 最新10件のみ保持
    st.session_state.recent_searches = st.session_state.recent_searches[:10]

def display_info_card(title: str, value: str, description: str = ""):
    """情報カードを表示"""
    st.metric(
        label=title,
        value=value,
        help=description
    )

def display_success_message(message: str):
    """成功メッセージを表示"""
    st.success(f"✅ {message}")

def display_error_message(message: str):
    """エラーメッセージを表示"""
    st.error(f"❌ {message}")

def safe_switch_page(page_path: str) -> None:
    """ページ遷移の安全版。ページが見つからない場合はエラーを出さずに警告を表示する。"""
    try:
        st.switch_page(page_path)
    except Exception:
        st.warning(f"ページが見つかりませんでした: {page_path}。SnowsightのApp filesに同一パスで存在するか確認してください。")

def load_announcements():
    """データベースからお知らせを取得"""
    try:
        result = session.sql("""
        SELECT * FROM application_db.application_schema.ANNOUNCEMENTS 
        WHERE show_flag = TRUE 
        ORDER BY priority, created_at DESC
        """).collect()
        return [row.as_dict() for row in result]
    except:
        return []

def render_announcements():
    """お知らせセクションを表示（DB版）"""
    # データベースからお知らせを取得
    announcements = load_announcements()
    
    # 現在日付でフィルタリング（表示期間内のもの）
    current_date = datetime.now().date()
    active_announcements = []
    
    for ann in announcements:
        # データベースから取得した日付をdateオブジェクトに変換
        try:
            if isinstance(ann["START_DATE"], str):
                start_date = datetime.strptime(ann["START_DATE"], "%Y-%m-%d").date()
            else:
                start_date = ann["START_DATE"]
                
            if isinstance(ann["END_DATE"], str):
                end_date = datetime.strptime(ann["END_DATE"], "%Y-%m-%d").date()
            else:
                end_date = ann["END_DATE"]
            
            # 期間内かどうかをチェック
            if start_date <= current_date <= end_date:
                active_announcements.append(ann)
        except (ValueError, TypeError):
            # 日付変換エラーの場合はスキップ
            continue
    
    # 優先度順でソート（数字が小さいほど上に表示）
    active_announcements = sorted(active_announcements, key=lambda x: x["PRIORITY"])
    
    if active_announcements:
        st.markdown("### 📢 お知らせ")
        
        for announcement in active_announcements:
            # 全幅表示用の横長スタイル
            # お知らせの種類に応じて色分け表示
            if announcement["ANNOUNCEMENT_TYPE"] == "info":
                st.info(f"**{announcement['TITLE']}**\n\n{announcement['MESSAGE']}")
            elif announcement["ANNOUNCEMENT_TYPE"] == "warning":
                st.warning(f"**{announcement['TITLE']}**\n\n{announcement['MESSAGE']}")
            elif announcement["ANNOUNCEMENT_TYPE"] == "error":
                st.error(f"**{announcement['TITLE']}**\n\n{announcement['MESSAGE']}")
            elif announcement["ANNOUNCEMENT_TYPE"] == "success":
                st.success(f"**{announcement['TITLE']}**\n\n{announcement['MESSAGE']}")
    else:
        # お知らせがない場合の表示
        st.markdown("### 📢 お知らせ")
        st.info("現在、表示するお知らせはありません。")

# お知らせ管理関数は保守・運用ページ（pages/5_admin.py）に移動

# =========================================================
# メインページコンテンツ
# =========================================================
def render_home_page():
    """ホームページを表示"""
    # ヘッダー部分
    st.markdown("""
    <div style="text-align: center; padding: 2rem 0;">
        <h1>❄️ Snowflakeデータ操作アプリ</h1>
        <p style="font-size: 1.2em; color: #666;">⚠️こちらはサンプルアプリです⚠️</p>
        <p style="font-size: 1.2em; color: #666;">簡単なGUI操作でデータが探索できます</p>
    </div>
    """, unsafe_allow_html=True)
    
    # 1. メイン機能カード
    st.markdown("### 🚀 何をしたいですか？")
    
    # 3つのメイン機能をカード形式で表示
    col1, col2, col3 = st.columns(3)
    
    with col1:
        with st.container():
            st.markdown("""
            <div style="border: 2px solid #ff6b6b; border-radius: 10px; padding: 1.5rem; text-align: center; background-color: #fff5f5;">
                <h3>🔍 定型検索</h3>
                <p>よく使う検索を<br>テンプレート化</p>
                <p style="color: #666; font-size: 0.9em;">保存済みの検索条件で<br>素早くデータ取得</p>
            </div>
            """, unsafe_allow_html=True)
            if st.button("定型検索を開く", key="main_standard", use_container_width=True, type="primary"):
                safe_switch_page("pages/1_standard_search.py")
    
    with col2:
        with st.container():
            st.markdown("""
            <div style="border: 2px solid #4ecdc4; border-radius: 10px; padding: 1.5rem; text-align: center; background-color: #f0fffe;">
                <h3>📊 非定型検索</h3>
                <p>自由な条件で<br>データ検索</p>
                <p style="color: #666; font-size: 0.9em;">テーブルやカラムを選んで<br>カスタム検索</p>
            </div>
            """, unsafe_allow_html=True)
            if st.button("非定型検索を開く", key="main_adhoc", use_container_width=True, type="primary"):
                safe_switch_page("pages/2_adhoc_search.py")
    
    with col3:
        with st.container():
            st.markdown("""
            <div style="border: 2px solid #cccccc; border-radius: 10px; padding: 1.5rem; text-align: center; background-color: #f5f5f5; opacity: 0.6;">
                <h3>🗣️ 自然言語検索</h3>
                <p>チャット形式でのデータ集計・<br>高度なAIアシスタント</p>
                <p style="color: #999; font-size: 0.9em;">⚠️ 現在この機能は<br>一時的に無効です</p>
            </div>
            """, unsafe_allow_html=True)
            st.button("Cortex Analystを開く", key="main_cortex", use_container_width=True, type="primary", disabled=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # 2. 追加機能（小さめのカード）
#    st.markdown("### ⚙️ その他")
#    col4, col5, col6, col7 = st.columns(4)
    st.markdown("### ⚙️ その他")
    col4, col5, col6 = st.columns(3)
    
    
    with col4:
        with st.container():
            st.markdown("""
            <div style="border: 1px solid #ddd; border-radius: 8px; padding: 1rem; text-align: center;">
                <h4>📥 データ取込</h4>
                <p style="font-size: 0.9em;">外部データの取り込み</p>
            </div>
            """, unsafe_allow_html=True)
            if st.button("データ取込を開く", key="main_ingest", use_container_width=True):
                safe_switch_page("pages/4_ingest.py")
    
    with col5:
        with st.container():
            st.markdown("""
            <div style="border: 1px solid #ddd; border-radius: 8px; padding: 1rem; text-align: center;">
                <h4>🔧 保守・運用</h4>
                <p style="font-size: 0.9em;">システム管理機能</p>
            </div>
            """, unsafe_allow_html=True)
            if st.button("保守・運用を開く", key="main_admin", use_container_width=True):
                safe_switch_page("pages/5_admin.py")
    
    with col6:
        with st.container():
            st.markdown("""
            <div style="border: 1px solid #ddd; border-radius: 8px; padding: 1rem; text-align: center;">
                <h4>📋 追加機能</h4>
                <p style="font-size: 0.9em;">RAGやデータカタログ機能など自由に開発できます</p>
            </div>
            """, unsafe_allow_html=True)
            st.info("🔧 お客様自身で自由にカスタマイズ可能です")
            
    # with col7:
    #     with st.container():
    #         st.markdown("""
    #         <div style="border: 1px solid #ddd; border-radius: 8px; padding: 1rem; text-align: center;">
    #             <h4>🔐 個人情報参照承認</h4>
    #             <p style="font-size: 0.9em;">個人情報アクセス申請</p>
    #         </div>
    #         """, unsafe_allow_html=True)
    #         if st.button("承認申請", key="main_personal_info", use_container_width=True):
    #             safe_switch_page("pages/6_personal_info_approval.py")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # 3. お知らせセクション
    render_announcements()
    
    # 4. 最近の検索履歴（簡略版）
    if st.session_state.recent_searches:
        st.markdown("---")
        st.markdown("### 📝 最近の実行履歴")
        
        # 最新3件のみ表示
        for i, search in enumerate(st.session_state.recent_searches[:3]):
            status_icon = "✅" if search['status'] == "完了" else "⚠️"
            time_str = search['timestamp'].strftime('%m/%d %H:%M')
            st.markdown(f"{status_icon} **{search['name']}** ({search['type']}) - {time_str}")
        
        if len(st.session_state.recent_searches) > 3:
            st.markdown(f"_... 他 {len(st.session_state.recent_searches) - 3}件_")

# =========================================================
# サイドバー設定
# =========================================================
def render_sidebar():
    """サイドバーを表示"""
    st.sidebar.header("🧭 メニュー")
    
    # DB/スキーマ選択セクション
    st.sidebar.markdown("### 🗄️ データベース選択")
    
    # データベース選択
    databases = get_available_databases()
    if databases:
        # 現在の選択を保持、なければ最初のDBを選択
        current_db = st.session_state.selected_database
        if current_db not in databases:
            current_db = databases[0] if databases else ""
        
        selected_db = st.sidebar.selectbox(
            "データベース",
            databases,
            index=databases.index(current_db) if current_db in databases else 0,
            key="sidebar_db_select"
        )
        
        # DBが変更された場合、スキーマをリセット
        if selected_db != st.session_state.selected_database:
            st.session_state.selected_database = selected_db
            st.session_state.selected_schema = ""  # スキーマをリセット
        
        # スキーマ選択
        schemas = get_available_schemas(st.session_state.selected_database)
        if schemas:
            current_schema = st.session_state.selected_schema
            if current_schema not in schemas:
                current_schema = schemas[0] if schemas else ""
            
            selected_schema = st.sidebar.selectbox(
                "スキーマ",
                schemas,
                index=schemas.index(current_schema) if current_schema in schemas else 0,
                key="sidebar_schema_select"
            )
            st.session_state.selected_schema = selected_schema
        else:
            st.sidebar.info("スキーマが見つかりません")
    else:
        st.sidebar.warning("データベースが見つかりません")
    
    st.sidebar.markdown("---")
    
    # メイン機能への直接ナビゲーション
    st.sidebar.markdown("### 📋 主要機能")
    
    if st.sidebar.button("🏠 ホーム", use_container_width=True):
        st.rerun()  # ホームページなので再読み込み
    
    if st.sidebar.button("🔍 定型検索", use_container_width=True):
        safe_switch_page("pages/1_standard_search.py")
    
    if st.sidebar.button("📊 非定型検索", use_container_width=True):
        safe_switch_page("pages/2_adhoc_search.py")
    
    st.sidebar.button("🗣️ 自然言語検索（準備中）", use_container_width=True, disabled=True)
    
    st.sidebar.markdown("---")
    
    # その他の機能
    st.sidebar.markdown("### ⚙️ その他")
    
    if st.sidebar.button("📥 データ取込", use_container_width=True):
        safe_switch_page("pages/4_ingest.py")
    
    if st.sidebar.button("🔧 保守・運用", use_container_width=True):
        safe_switch_page("pages/5_admin.py")
    
#    if st.sidebar.button("🔐 個人情報参照承認", use_container_width=True):
#        safe_switch_page("pages/6_personal_info_approval.py")
    
    st.sidebar.markdown("---")
    
    # 簡単な情報表示
    st.sidebar.markdown("### ℹ️ 選択中の情報")
    
    # 選択されたDB/スキーマの情報を表示
    if st.session_state.selected_database and st.session_state.selected_schema:
        try:
            # 選択されたスキーマ内のテーブル数を取得
            tables = get_available_tables_dynamic(
                st.session_state.selected_database, 
                st.session_state.selected_schema
            )
            views = get_available_views_dynamic(
                st.session_state.selected_database,
                st.session_state.selected_schema
            )
            
            st.sidebar.info(
                f"📊 **選択中のデータベース情報**\n\n"
                f"**データベース**: {st.session_state.selected_database}\n\n"
                f"**スキーマ**: {st.session_state.selected_schema}\n\n"
                f"**テーブル数**: {len(tables)}個\n\n"
                f"**ビュー数**: {len(views)}個"
            )
        except Exception as e:
            st.sidebar.info("📊 利用可能テーブル: 確認中...")
            st.sidebar.caption(f"エラー: {str(e)}")
    else:
        st.sidebar.info("📊 データベースとスキーマを選択してください")
    
    # お気に入りの簡単表示
    if st.session_state.favorites:
        fav_count = len(st.session_state.favorites)
        st.sidebar.info(f"⭐ お気に入り: {fav_count}個")
    
    # 履歴の簡単表示
    if st.session_state.recent_searches:
        recent_count = len(st.session_state.recent_searches)
        st.sidebar.info(f"📝 実行履歴: {recent_count}個")

# =========================================================
# メインアプリケーション
# =========================================================
def main():
    """メインアプリケーション"""
    
    # サイドバーを表示
    render_sidebar()
    
    # メインページを表示
    render_home_page()
    
    # フッター（簡潔版）
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666; padding: 1rem;'>"
        "📊 Streamlitデータアプリ - ©Snowflake合同会社</div>", 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main() 
