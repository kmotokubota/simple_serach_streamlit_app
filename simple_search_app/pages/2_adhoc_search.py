# =========================================================
# Snowflakeデータ操作アプリケーション
# 非定型検索（明細型検索）ページ
# =========================================================
# Created by kdaigo
# 最終更新: 2025/09/24
# =========================================================

import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit

st.set_page_config(layout="wide", page_title="📊 非定型検索", page_icon="📊")

@st.cache_resource
def get_snowflake_session():
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# 定数定義: データスキーマ（デフォルト値として保持）
# =========================================================
DEFAULT_DATA_SCHEMA = "bank_db.bank_schema"
APP_DATA_SCHEMA = "application_db.application_schema"
# 検索対象から除外するシステムテーブル
SYSTEM_TABLES = {"STANDARD_SEARCH_OBJECTS", "ADHOC_SEARCH_OBJECTS", "ANNOUNCEMENTS"}
# 検索対象から除外するテーブル名のプレフィックス
EXCLUDED_PREFIXES = ("SNOWPARK_TEMP_TABLE_",)

# =========================================================
# DB/スキーマ動的選択のヘルパー関数
# =========================================================
@st.cache_data(ttl=60, show_spinner=False)
def get_available_databases():
    """アクセス可能なデータベース一覧を取得"""
    try:
        result = session.sql("SHOW DATABASES").collect()
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
        tables = []
        for row in result:
            name = row['name']
            if name not in SYSTEM_TABLES and not name.upper().startswith(EXCLUDED_PREFIXES):
                tables.append(name)
        return sorted(tables)
    except:
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

def get_current_data_schema():
    """現在選択されているデータスキーマを取得（DB.SCHEMA形式）"""
    if st.session_state.get('selected_database') and st.session_state.get('selected_schema'):
        return f"{st.session_state.selected_database}.{st.session_state.selected_schema}"
    return DEFAULT_DATA_SCHEMA

def quote_identifier(identifier: str) -> str:
    """SQL識別子（テーブル名、カラム名）を適切にクォートする"""
    if not identifier:
        return identifier
    # 前後の空白、改行、特殊文字をトリム
    identifier = identifier.strip().strip('\n\r\t')
    # 既にクォートされている場合はそのまま返す
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier
    # 内部のダブルクォートをエスケープ
    escaped_identifier = identifier.replace('"', '""')
    return f'"{escaped_identifier}"'

# セッション状態の初期化（3テーブル結合対応）
if 'selected_table1' not in st.session_state:
    st.session_state.selected_table1 = ""
if 'selected_table2' not in st.session_state:
    st.session_state.selected_table2 = ""
if 'selected_table3' not in st.session_state:
    st.session_state.selected_table3 = ""
if 'join_key1' not in st.session_state:
    st.session_state.join_key1 = ""
if 'join_key2' not in st.session_state:
    st.session_state.join_key2 = ""
if 'join_key3' not in st.session_state:
    st.session_state.join_key3 = ""
if 'join_type1' not in st.session_state:
    st.session_state.join_type1 = "INNER JOIN"
if 'join_type2' not in st.session_state:
    st.session_state.join_type2 = "INNER JOIN"
if 'search_result_df' not in st.session_state:
    st.session_state.search_result_df = None
if 'work_table_selection' not in st.session_state:
    st.session_state.work_table_selection = ""
# WHERE条件とソート条件の管理（1_standard_search.pyと同じロジック）
if 'adhoc_where_conditions_list' not in st.session_state:
    st.session_state.adhoc_where_conditions_list = []
if 'adhoc_order_by_conditions_list' not in st.session_state:
    st.session_state.adhoc_order_by_conditions_list = []
if 'adhoc_group_by_conditions_list' not in st.session_state:
    st.session_state.adhoc_group_by_conditions_list = []
if 'enable_3table_join' not in st.session_state:
    st.session_state.enable_3table_join = False
if 'join_key2_for_join2' not in st.session_state:
    st.session_state.join_key2_for_join2 = ""
if 'adhoc_selected_columns' not in st.session_state:
    st.session_state.adhoc_selected_columns = set()
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = "📄 検索結果"

# DB/スキーマ選択のセッション状態
if 'selected_database' not in st.session_state:
    st.session_state.selected_database = ""
if 'selected_schema' not in st.session_state:
    st.session_state.selected_schema = ""

# check_table_exists関数は削除 - setup SQLで事前作成済み

def is_excluded_table_early(table_name: str) -> bool:
    """除外対象のテーブルかどうかを判定（get_available_tables用）"""
    if table_name in SYSTEM_TABLES:
        return True
    if table_name.upper().startswith(EXCLUDED_PREFIXES):
        return True
    return False

def get_available_tables():
    """選択されたスキーマからテーブル名を取得"""
    selected_db = st.session_state.get('selected_database', '')
    selected_schema = st.session_state.get('selected_schema', '')
    
    if selected_db and selected_schema:
        return get_available_tables_dynamic(selected_db, selected_schema)
    else:
        return []

def get_table_columns(table_name: str):
    try:
        quoted_table = f'"{table_name}"' if not table_name.startswith('"') else table_name
        # テーブルのスキーマを動的に判定
        schema = get_table_schema(table_name)
        result = session.sql(f"DESCRIBE TABLE {schema}.{quoted_table}").collect()
        return [{'name': row['name'], 'type': row['type']} for row in result]
    except:
        return []

def is_date_type(data_type: str) -> bool:
    """データ型が日付型かどうかを判定する"""
    if not data_type:
        return False
    
    data_type_upper = data_type.upper()
    date_types = [
        'DATE', 'DATETIME', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ',
        'TIME', 'DATETIME_NTZ', 'DATETIME_LTZ', 'DATETIME_TZ'
    ]
    
    return any(date_type in data_type_upper for date_type in date_types)

def is_date_like_column(col_name: str, data_type: str) -> bool:
    """カラムが日付データを含む可能性があるかを判定する（型とカラム名の両方をチェック）"""
    # まずデータ型で判定
    if is_date_type(data_type):
        return True
    
    # カラム名に日付を示すキーワードが含まれている場合（VARCHAR型でも日付として扱う）
    col_name_upper = col_name.upper()
    date_keywords = [
        'DATE', 'DT', '日付', '年月日', 'YMD', 'YYYYMMDD',
        '_AT', 'CREATED', 'UPDATED', 'REGISTERED', 'TIMESTAMP',
        '登録日', '更新日', '作成日', '開始日', '終了日', '取引日', '発生日'
    ]
    
    return any(keyword in col_name_upper for keyword in date_keywords)

def is_numeric_type(data_type: str) -> bool:
    """データ型が数値型かどうかを判定する"""
    if not data_type:
        return False
    
    data_type_upper = data_type.upper()
    numeric_types = [
        'NUMBER', 'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT',
        'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'
    ]
    
    return any(numeric_type in data_type_upper for numeric_type in numeric_types)


@st.cache_data(ttl=300, show_spinner=False)
def get_table_schema(table_name: str) -> str:
    """テーブルがどのスキーマに存在するかを判定して返す"""
    # まず選択中のスキーマを確認
    current_schema = get_current_data_schema()
    try:
        quoted_table = f'"{table_name}"' if not table_name.startswith('"') else table_name
        session.sql(f"DESCRIBE TABLE {current_schema}.{quoted_table}").collect()
        return current_schema
    except:
        pass
    # 次にapplication_db.application_schemaを確認（システムテーブル用）
    try:
        quoted_table = f'"{table_name}"' if not table_name.startswith('"') else table_name
        session.sql(f"DESCRIBE TABLE {APP_DATA_SCHEMA}.{quoted_table}").collect()
        return APP_DATA_SCHEMA
    except:
        pass
    return current_schema  # デフォルトは選択中のスキーマ

def is_excluded_table(table_name: str) -> bool:
    """除外対象のテーブルかどうかを判定"""
    if table_name in SYSTEM_TABLES:
        return True
    if table_name.upper().startswith(EXCLUDED_PREFIXES):
        return True
    return False

def get_available_relations():
    """選択されたスキーマからテーブルとビュー名を取得"""
    tables = []
    views = []
    
    # 選択されたDB/スキーマからテーブル/ビューを取得
    selected_db = st.session_state.get('selected_database', '')
    selected_schema = st.session_state.get('selected_schema', '')
    
    if selected_db and selected_schema:
        # テーブル取得
        tables = get_available_tables_dynamic(selected_db, selected_schema)
        # ビュー取得
        views = get_available_views_dynamic(selected_db, selected_schema)
    else:
        st.warning("⚠️ サイドバーでデータベースとスキーマを選択してください")
    
    # ラベル付けして返す
    labeled = [f"[TABLE] {t}" for t in tables] + [f"[VIEW] {v}" for v in views]
    return sorted(labeled)

def parse_relation_label(label: str) -> str:
    """[TABLE]/[VIEW] ラベルからオブジェクト名のみ取り出す"""
    return label.split(' ', 1)[1] if ' ' in label else label


# create_adhoc_search_table関数は削除 - setup SQLで事前作成済み

def save_adhoc_search_object(object_data: dict):
    """非定型検索オブジェクトを保存（新構成版）"""
    try:
        
        # NULL値のハンドリング
        table1_name = object_data.get('table1_name') or 'テーブル1'
        table2_name = object_data.get('table2_name') or 'テーブル2'
        join_type = object_data.get('join_type') or 'INNER JOIN'
        join_key1 = object_data.get('join_key1') or 'key1'
        join_key2 = object_data.get('join_key2') or 'key2'
        
        session.sql("""
        INSERT INTO application_db.application_schema.ADHOC_SEARCH_OBJECTS (
            object_id, object_name, description, table1_name, table2_name,
            join_type, join_key1, join_key2, search_query, created_by, is_favorite
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_USER(), ?)
        """, params=[
            object_data['object_id'],
            object_data['object_name'],
            object_data['description'],
            table1_name,
            table2_name,
            join_type,
            join_key1,
            join_key2,
            object_data['search_query'],
            object_data.get('is_favorite', False)
        ]).collect()
        return True
    except Exception as e:
        st.error(f"保存エラー: {str(e)}")
        return False

def update_adhoc_execution_count(object_id: str):
    """非定型検索オブジェクトの実行回数を更新する"""
    try:
        session.sql("""
        UPDATE application_db.application_schema.ADHOC_SEARCH_OBJECTS 
        SET execution_count = execution_count + 1, 
            last_executed = CURRENT_TIMESTAMP()
        WHERE object_id = ?
        """, params=[object_id]).collect()
        return True
    except Exception as e:
        st.error(f"実行回数更新エラー: {str(e)}")
        return False

def toggle_adhoc_favorite(object_id: str):
    """お気に入り状態を切り替える"""
    try:
        session.sql("""
        UPDATE application_db.application_schema.ADHOC_SEARCH_OBJECTS 
        SET is_favorite = NOT is_favorite,
            updated_at = CURRENT_TIMESTAMP()
        WHERE object_id = ?
        """, params=[object_id]).collect()
        return True
    except Exception as e:
        st.error(f"お気に入り更新エラー: {str(e)}")
        return False

def load_adhoc_search_objects():
    """非定型検索オブジェクト一覧を取得"""
    try:
        result = session.sql("SELECT * FROM application_db.application_schema.ADHOC_SEARCH_OBJECTS ORDER BY created_at DESC").collect()
        return [row.as_dict() for row in result]
    except Exception as e:
        st.error(f"データ読み込みエラー: {str(e)}")
        return []

def save_result_as_work_table(df: pd.DataFrame, table_name: str):
    """検索結果を作業テーブルとして保存"""
    try:
        # デバッグ情報を追加
        print(f"[DEBUG] 保存対象テーブル名: {table_name}")
        print(f"[DEBUG] データフレームサイズ: {len(df)} 行, {len(df.columns)} 列")
        
        # Snowpark DataFrameに変換してSnowflakeに保存
        from snowflake.snowpark import Session
        snowpark_df = session.create_dataframe(df)
        
        # テーブル名を明示的に指定して保存
        snowpark_df.write.mode("overwrite").save_as_table(table_name)
        
        # 保存成功（詳細なメッセージは呼び出し元で表示）
        return True, "保存完了"
            
    except Exception as e:
        print(f"[ERROR] 保存エラー: {str(e)}")
        return False, f"保存エラー: {str(e)}"

def create_snowflake_task_for_adhoc(task_name: str, schedule: str, search_query: str, work_table_name: str):
    """非定型検索用のSnowflakeタスクを作成（1_standard_search.pyを参考に実装）"""
    try:
        # タスク名をクォートして特殊文字に対応
        quoted_task_name = f'"{task_name}"'
        quoted_work_table = quote_identifier(work_table_name)
        
        # 検索クエリをエスケープ
        escaped_query = search_query.replace("'", "''")
        
        create_task_sql = f"""
        CREATE OR REPLACE TASK {quoted_task_name}
        WAREHOUSE = COMPUTE_WH
        SCHEDULE = 'USING CRON {schedule} Asia/Tokyo'
        AS
        CREATE OR REPLACE TABLE {quoted_work_table} AS ({escaped_query})
        """
        session.sql(create_task_sql).collect()
        
        # タスクを有効化
        session.sql(f"ALTER TASK {quoted_task_name} RESUME").collect()
        
        return True, "タスクを作成し、有効化しました"
    except Exception as e:
        return False, str(e)

def get_scheduled_tasks_adhoc():
    """登録済みのタスク一覧を取得（非定型検索用）"""
    try:
        result = session.sql("SHOW TASKS").collect()
        tasks = []
        for row in result:
            task_info = row.as_dict()
            # 非定型検索関連のタスクのみフィルター
            if task_info.get('name', '').startswith('adhoc_'):
                tasks.append(task_info)
        return tasks
    except Exception as e:
        st.error(f"タスク一覧取得エラー: {str(e)}")
        return []

def suspend_task_adhoc(task_name: str):
    """タスクを一時停止"""
    try:
        quoted_task_name = f'"{task_name}"'
        session.sql(f"ALTER TASK {quoted_task_name} SUSPEND").collect()
        return True, "タスクを一時停止しました"
    except Exception as e:
        return False, str(e)

def resume_task_adhoc(task_name: str):
    """タスクを再開"""
    try:
        quoted_task_name = f'"{task_name}"'
        session.sql(f"ALTER TASK {quoted_task_name} RESUME").collect()
        return True, "タスクを再開しました"
    except Exception as e:
        return False, str(e)

def execute_query(search_query: str, limit_rows: int = 1000):
    """クエリを実行し、結果をセッション状態に保存する"""
    try:
        final_query = search_query.strip()
        
        # LIMIT句がない場合のみ追加（デバッグ情報付き）
        if "LIMIT" not in final_query.upper():
            final_query = f"{final_query} LIMIT {int(limit_rows)}"
            st.info(f"🔍 LIMIT句を追加しました: {limit_rows}行")
        else:
            st.info(f"🔍 既存のLIMIT句を使用します")
        
        # デバッグ用: 最終的なクエリを表示
        st.code(final_query, language="sql")
        
        with st.spinner("検索実行中..."):
            # 件数チェック（LIMIT句を考慮）
            try:
                test_query = f"SELECT COUNT(*) FROM ({final_query.replace(f'LIMIT {limit_rows}', '')})"
                row_count = session.sql(test_query).collect()[0][0]
                
                if row_count > 5000:
                    st.warning(f"検索結果が5,000行を超えています。表示に時間がかかる場合があります。総件数: {row_count} 行")
                elif row_count == 0:
                    st.warning("検索条件に該当するデータがありません。")
                    return
                else:
                    st.info(f"📊 総件数: {row_count} 行、制限: {limit_rows} 行")
            except Exception as count_error:
                st.warning(f"件数チェックエラー: {str(count_error)}")

            # データ取得実行
            df_result = session.sql(final_query).to_pandas()
            st.session_state.search_result_df = df_result
            st.success(f"✅ 実際の取得件数: {len(df_result)} 行。下部の『📄 出力結果』に表示しました。")

    except Exception as e:
        st.error(f"検索エラー: {str(e)}")
        st.write("実行クエリの参考:")
        st.code(final_query if 'final_query' in locals() else search_query, language="sql")

# =========================================================
# サイドバー: DB/スキーマ選択
# =========================================================
st.sidebar.header("🗄️ データベース選択")

# データベース選択
databases = get_available_databases()
if databases:
    current_db = st.session_state.selected_database
    if current_db not in databases:
        current_db = databases[0] if databases else ""
    
    selected_db = st.sidebar.selectbox(
        "データベース",
        databases,
        index=databases.index(current_db) if current_db in databases else 0,
        key="adhoc_search_db_select"
    )
    
    if selected_db != st.session_state.selected_database:
        st.session_state.selected_database = selected_db
        st.session_state.selected_schema = ""
        st.rerun()
    
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
            key="adhoc_search_schema_select"
        )
        if selected_schema != st.session_state.selected_schema:
            st.session_state.selected_schema = selected_schema
            st.rerun()
    else:
        st.sidebar.info("スキーマが見つかりません")
else:
    st.sidebar.warning("データベースが見つかりません")

# 選択中の情報を表示
if st.session_state.selected_database and st.session_state.selected_schema:
    tables = get_available_tables_dynamic(st.session_state.selected_database, st.session_state.selected_schema)
    views = get_available_views_dynamic(st.session_state.selected_database, st.session_state.selected_schema)
    st.sidebar.info(f"📊 テーブル: {len(tables)}個 / ビュー: {len(views)}個")

st.sidebar.markdown("---")

# =========================================================
# アプリケーション本体
# =========================================================

st.title("📊 非定型検索")
st.header("テーブル結合による柔軟なデータ検索")

# サイドバーメニューは上部ナビゲーションと重複するため削除

# =========================================================
# STEP 1: テーブル選択
# =========================================================

st.markdown("### 🔗 STEP 1: テーブル選択")

# 2テーブル結合のみに固定（サンプルアプリのため）
st.session_state.enable_3table_join = False

# 2テーブル用レイアウト
colL, colR = st.columns([1, 1])

# テーブル1
with colL:
    st.markdown("#### 📋 テーブル1（メインテーブル）")
    available_relations = get_available_relations()
    
    if available_relations:
        selected_relation1 = st.selectbox("テーブル1を選択", [""] + available_relations, key="table1_selector")
        if selected_relation1:
            st.session_state.selected_table1 = parse_relation_label(selected_relation1)
            
            if st.session_state.selected_table1:
                st.markdown("##### 🔍 テーブル1 カラム情報")
                basic_cols1 = get_table_columns(st.session_state.selected_table1)
                cols1_info = [{'name': c['name'], 'type': c['type']} for c in basic_cols1]
                
                if cols1_info:
                    display_data1 = []
                    for c in cols1_info:
                        display_row = {
                            'カラム名': c['name'],
                            'データ型': c['type']
                        }
                        display_data1.append(display_row)
                    
                    df1 = pd.DataFrame(display_data1)
                    
                    column_config1 = {
                        "カラム名": st.column_config.TextColumn("カラム名", width="medium"),
                        "データ型": st.column_config.TextColumn("データ型", width="small")
                    }
                    
                    st.dataframe(df1, column_config=column_config1, use_container_width=True, hide_index=True)

# テーブル2
with colR:
    st.markdown("#### 📋 テーブル2（結合テーブル）")
    excluded_tables = [st.session_state.selected_table1]
    
    if available_relations:
        table_options = [""] + [rel for rel in available_relations if parse_relation_label(rel) not in excluded_tables]
        selected_relation = st.selectbox("テーブル2を選択", table_options, key="table2_selector", 
                                        disabled=not st.session_state.selected_table1)
        
        if selected_relation:
            st.session_state.selected_table2 = parse_relation_label(selected_relation)
            
            if st.session_state.selected_table2:
                st.markdown("##### 🔍 テーブル2 カラム情報")
                basic_cols = get_table_columns(st.session_state.selected_table2)
                cols_info = [{'name': c['name'], 'type': c['type']} for c in basic_cols]
                
                if cols_info:
                    display_data = []
                    for c in cols_info:
                        display_row = {
                            'カラム名': c['name'],
                            'データ型': c['type']
                        }
                        display_data.append(display_row)
                    
                    df = pd.DataFrame(display_data)
                    
                    column_config = {
                        "カラム名": st.column_config.TextColumn("カラム名", width="medium"),
                        "データ型": st.column_config.TextColumn("データ型", width="small")
                    }
                    
                    st.dataframe(df, column_config=column_config, use_container_width=True, hide_index=True)

# =========================================================
# STEP 2: 結合条件設定
# =========================================================
st.markdown("---")
st.markdown("### 🔗 STEP 2: 結合条件設定")

# 2テーブル結合の場合
if not st.session_state.enable_3table_join and st.session_state.selected_table1 and st.session_state.selected_table2:
    
    col1, col2, col3 = st.columns([2, 2, 2])
    
    with col1:
        st.session_state.join_type1 = st.selectbox(
            "結合タイプ", 
            ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"],
            help="INNER: 両方にあるレコードのみ、LEFT: テーブル1中心",
            key="join_type1_2table"
        )
    
    with col2:
        table1_cols = [c['name'] for c in get_table_columns(st.session_state.selected_table1)]
        
        options1 = [""] + table1_cols
        current_index1 = 0
        if st.session_state.join_key1 in options1:
            current_index1 = options1.index(st.session_state.join_key1)
        
        selected_key1 = st.selectbox(
            f"テーブル1キー ({st.session_state.selected_table1})",
            options1,
            index=current_index1,
            key="join_key1_select_2table"
        )
        st.session_state.join_key1 = selected_key1
    
    with col3:
        table2_cols = [c['name'] for c in get_table_columns(st.session_state.selected_table2)]
        
        options2 = [""] + table2_cols
        current_index2 = 0
        if st.session_state.join_key2 in options2:
            current_index2 = options2.index(st.session_state.join_key2)
        
        selected_key2 = st.selectbox(
            f"テーブル2キー ({st.session_state.selected_table2})",
            options2,
            index=current_index2,
            key="join_key2_select_2table"
        )
        st.session_state.join_key2 = selected_key2
    
    # 結合条件表示（2テーブル）
    if st.session_state.join_key1 and st.session_state.join_key2:
        st.markdown("#### 🔗 設定された結合条件")
        st.info(f"**{st.session_state.join_type1}**: {st.session_state.selected_table1}.{st.session_state.join_key1} = {st.session_state.selected_table2}.{st.session_state.join_key2}")

# 3テーブル結合の場合
elif st.session_state.enable_3table_join and st.session_state.selected_table1 and st.session_state.selected_table2 and st.session_state.selected_table3:
    
    st.markdown("#### 🔗 1番目の結合: テーブル1 ⇄ テーブル2")
    col1, col2, col3 = st.columns([2, 2, 2])
    
    with col1:
        st.session_state.join_type1 = st.selectbox(
            "1番目の結合タイプ", 
            ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"],
            key="join_type1_3table"
        )
    
    with col2:
        table1_cols = [c['name'] for c in get_table_columns(st.session_state.selected_table1)]
        
        options1 = [""] + table1_cols
        current_index1 = 0
        if st.session_state.join_key1 in options1:
            current_index1 = options1.index(st.session_state.join_key1)
        
        selected_key1 = st.selectbox(
            f"テーブル1キー ({st.session_state.selected_table1})",
            options1,
            index=current_index1,
            key="join_key1_select_3table"
        )
        st.session_state.join_key1 = selected_key1
    
    with col3:
        table2_cols = [c['name'] for c in get_table_columns(st.session_state.selected_table2)]
        
        options2 = [""] + table2_cols
        current_index2 = 0
        if st.session_state.join_key2 in options2:
            current_index2 = options2.index(st.session_state.join_key2)
        
        selected_key2 = st.selectbox(
            f"テーブル2キー ({st.session_state.selected_table2})",
            options2,
            index=current_index2,
            key="join_key2_select_3table"
        )
        st.session_state.join_key2 = selected_key2
    
    st.markdown("#### 🔗 2番目の結合: テーブル2 ⇄ テーブル3")
    col5, col6, col7 = st.columns([2, 2, 2])
    
    with col5:
        st.session_state.join_type2 = st.selectbox(
            "2番目の結合タイプ", 
            ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"],
            key="join_type2_3table"
        )
    
    with col6:
        # テーブル2のキー（2番目の結合用）
        options2_for_join2 = [""] + table2_cols
        current_index2_for_join2 = 0
        if st.session_state.join_key2_for_join2 in options2_for_join2:
            current_index2_for_join2 = options2_for_join2.index(st.session_state.join_key2_for_join2)
        
        selected_key2_for_join2 = st.selectbox(
            f"テーブル2キー2 ({st.session_state.selected_table2})",
            options2_for_join2,
            index=current_index2_for_join2,
            key="join_key2_for_join2_select"
        )
        st.session_state.join_key2_for_join2 = selected_key2_for_join2
    
    with col7:
        table3_cols = [c['name'] for c in get_table_columns(st.session_state.selected_table3)]
        
        options3 = [""] + table3_cols
        current_index3 = 0
        if st.session_state.join_key3 in options3:
            current_index3 = options3.index(st.session_state.join_key3)
        
        selected_key3 = st.selectbox(
            f"テーブル3キー ({st.session_state.selected_table3})",
            options3,
            index=current_index3,
            key="join_key3_select"
        )
        st.session_state.join_key3 = selected_key3
    
    # 結合条件表示（3テーブル）
    if st.session_state.join_key1 and st.session_state.join_key2 and st.session_state.join_key3 and st.session_state.join_key2_for_join2:
        st.markdown("#### 🔗 設定された結合条件")
        st.info(f"**1番目**: {st.session_state.join_type1} {st.session_state.selected_table1}.{st.session_state.join_key1} = {st.session_state.selected_table2}.{st.session_state.join_key2}")
        st.info(f"**2番目**: {st.session_state.join_type2} {st.session_state.selected_table2}.{st.session_state.join_key2_for_join2} = {st.session_state.selected_table3}.{st.session_state.join_key3}")

else:
    # 進行状況ガイド
    if st.session_state.enable_3table_join:
        if not st.session_state.selected_table1:
            st.info("🔸 STEP 1: テーブル1を選択してください")
        elif not st.session_state.selected_table2:
            st.info("🔸 STEP 1: テーブル2を選択してください")
        elif not st.session_state.selected_table3:
            st.info("🔸 STEP 1: テーブル3を選択してください")
    else:
        if not st.session_state.selected_table1:
            st.info("🔸 STEP 1: テーブル1を選択してください")
        elif not st.session_state.selected_table2:
            st.info("🔸 STEP 1: テーブル2を選択してください")

# =========================================================
# STEP 3: 出力カラム選択
# =========================================================
st.markdown("---")
st.markdown("### 📋 STEP 3: 出力カラム選択")

# 2テーブルまたは3テーブル結合でカラム選択可能な場合
if ((not st.session_state.enable_3table_join and st.session_state.selected_table1 and st.session_state.selected_table2) or
    (st.session_state.enable_3table_join and st.session_state.selected_table1 and st.session_state.selected_table2 and st.session_state.selected_table3)):
    
    # 全カラム情報を収集
    all_columns = []
    
    # テーブル1のカラム
    cols1 = get_table_columns(st.session_state.selected_table1)
    for col in cols1:
        all_columns.append({
            'display_name': f"[T1] {col['name']}",
            'sql_name': f"t1.{quote_identifier(col['name'])}",
            'original_name': col['name'],
            'table': 'T1',
            'type': col['type']
        })
    
    # テーブル2のカラム
    cols2 = get_table_columns(st.session_state.selected_table2)
    for col in cols2:
        all_columns.append({
            'display_name': f"[T2] {col['name']}",
            'sql_name': f"t2.{quote_identifier(col['name'])}",
            'original_name': col['name'],
            'table': 'T2',
            'type': col['type']
        })
    
    # テーブル3のカラム（3テーブルモードの場合）
    if st.session_state.enable_3table_join and st.session_state.selected_table3:
        cols3 = get_table_columns(st.session_state.selected_table3)
        for col in cols3:
            all_columns.append({
                'display_name': f"[T3] {col['name']}",
                'sql_name': f"t3.{quote_identifier(col['name'])}",
                'original_name': col['name'],
                'table': 'T3',
                'type': col['type']
            })
    
    # 重複カラム名への対応
    table1_col_names = {c['name'] for c in cols1}
    table2_col_names = {c['name'] for c in cols2}
    duplicate_cols = table1_col_names & table2_col_names
    
    if st.session_state.enable_3table_join and st.session_state.selected_table3:
        cols3 = get_table_columns(st.session_state.selected_table3)
        table3_col_names = {c['name'] for c in cols3}
        duplicate_cols.update(table1_col_names & table3_col_names)
        duplicate_cols.update(table2_col_names & table3_col_names)
    
    # 結合キーを特定（業務的に不要なため除外）
    join_keys_to_exclude = set()
    if st.session_state.join_key1:
        join_keys_to_exclude.add(st.session_state.join_key1)
    if st.session_state.join_key2:
        join_keys_to_exclude.add(st.session_state.join_key2)
    if st.session_state.enable_3table_join and st.session_state.join_key3:
        join_keys_to_exclude.add(st.session_state.join_key3)
    if st.session_state.enable_3table_join and st.session_state.join_key2_for_join2:
        join_keys_to_exclude.add(st.session_state.join_key2_for_join2)
    
    # 重複カラムから結合キーを除外（結合キーは重複でも問題ない）
    duplicate_cols_excluding_join_keys = duplicate_cols - join_keys_to_exclude
    
    # 結合キー除外後のカラムのみを処理
    processed_columns = []
    excluded_count = 0
    
    for col_info in all_columns:
        # 結合キーは除外（業務観点で不要）
        if col_info['original_name'] in join_keys_to_exclude:
            excluded_count += 1
            continue
        
        # 重複カラムは別名で処理（結合キー除外後）
        if col_info['original_name'] in duplicate_cols_excluding_join_keys:
            alias_name = f"{col_info['table'].lower()}_{col_info['original_name']}"
            col_info['sql_name'] = f"{col_info['sql_name']} AS {quote_identifier(alias_name)}"
            col_info['display_name'] = f"[{col_info['table']}] {col_info['original_name']} (→{alias_name})"
        
        processed_columns.append(col_info)
    
    # 除外したキー情報を表示
    if excluded_count > 0:
        st.info(f"🔗 結合キー {excluded_count}個を自動除外しました（業務観点で不要なため）")
        with st.expander("🔍 除外された結合キー", expanded=False):
            for key in sorted(join_keys_to_exclude):
                st.write(f"- `{key}`")
    
    # カラム選択UI
    col_select1, col_select2 = st.columns([1, 1])
    
    with col_select1:
        if st.button("✅ 全選択", key="select_all_adhoc_cols"):
            st.session_state.adhoc_selected_columns = {col['sql_name'] for col in processed_columns}
            st.rerun()
        
        if st.button("🧹 全解除", key="clear_all_adhoc_cols"):
            st.session_state.adhoc_selected_columns = set()
            st.rerun()
    
    with col_select2:
        filter_text = st.text_input("カラム検索（部分一致）", key="adhoc_col_filter")
    
    # カラム一覧表示（選択機能付き）
    if filter_text:
        filtered_columns = [col for col in processed_columns if filter_text.lower() in col['display_name'].lower()]
    else:
        filtered_columns = processed_columns
    
    if filtered_columns:
        # リスト形式UIで一度のクリックで確実に選択できるように改善
        display_data = []
        for col in filtered_columns:
            is_selected = col['sql_name'] in st.session_state.adhoc_selected_columns
            display_data.append({
                '選択': is_selected,
                'カラム名': col['display_name'],
                'データ型': col['type'],
                'テーブル': col['table']
            })
        
        df_cols = pd.DataFrame(display_data)
        
        column_config = {
            "選択": st.column_config.CheckboxColumn("選択", help="出力するカラムを選択", default=False),
            "カラム名": st.column_config.TextColumn("カラム名", width="large"),
            "データ型": st.column_config.TextColumn("データ型", width="small"),
            "テーブル": st.column_config.TextColumn("テーブル", width="small")
        }
        
        # キーを動的に生成して状態管理を改善
        editor_key = f"adhoc_column_selection_editor_{len(filtered_columns)}"
        
        edited_df = st.data_editor(
            df_cols,
            column_config=column_config,
            hide_index=True,
            use_container_width=True,
            key=editor_key,
            disabled=["カラム名", "データ型", "テーブル"]  # 選択以外は編集不可
        )
        
        # 編集結果を即座にセッション状態に反映
        new_selected_columns = set()
        for idx, row in edited_df.iterrows():
            if row['選択']:
                # 対応するカラムのsql_nameを取得
                for col in filtered_columns:
                    if col['display_name'] == row['カラム名']:
                        new_selected_columns.add(col['sql_name'])
                        break
        
        # セッション状態を更新（変更があった場合のみ）
        if new_selected_columns != st.session_state.adhoc_selected_columns:
            st.session_state.adhoc_selected_columns = new_selected_columns
            st.rerun()  # 状態変更を即座に反映
        
        # 選択状況の表示
        if st.session_state.adhoc_selected_columns:
            st.success(f"✅ 選択中: {len(st.session_state.adhoc_selected_columns)}個のカラム")
            
            # 選択されたカラムの詳細表示（確認用）
            with st.expander("🔍 選択されたカラム一覧", expanded=False):
                selected_details = []
                for col in filtered_columns:
                    if col['sql_name'] in st.session_state.adhoc_selected_columns:
                        selected_details.append({
                            'カラム名': col['display_name'],
                            'データ型': col['type'],
                            'テーブル': col['table']
                        })
                
                if selected_details:
                    df_selected = pd.DataFrame(selected_details)
                    st.dataframe(df_selected, use_container_width=True, hide_index=True)
        else:
            st.info("出力するカラムを選択してください")
    
    # 選択されたカラムに基づく重複カラム警告
    if st.session_state.adhoc_selected_columns:
        # 選択されたカラムから重複を検出
        selected_col_names_t1 = set()
        selected_col_names_t2 = set()
        
        for selected_col in st.session_state.adhoc_selected_columns:
            if selected_col.startswith('t1.'):
                col_part = selected_col[3:]  # "t1." を除去
                if ' AS ' in col_part:
                    col_part = col_part.split(' AS ')[0]
                col_name = col_part.strip('"')
                selected_col_names_t1.add(col_name)
            elif selected_col.startswith('t2.'):
                col_part = selected_col[3:]  # "t2." を除去
                if ' AS ' in col_part:
                    col_part = col_part.split(' AS ')[0]
                col_name = col_part.strip('"')
                selected_col_names_t2.add(col_name)
        
        # 選択されたカラム間での重複をチェック
        selected_duplicate_cols = selected_col_names_t1 & selected_col_names_t2
        
        if selected_duplicate_cols:
            st.warning(f"⚠️ 選択されたカラムで重複検出: {len(selected_duplicate_cols)}個のカラムに別名が付与されます。")
            with st.expander("🔍 重複カラム詳細", expanded=False):
                for dup_col in sorted(selected_duplicate_cols):
                    st.write(f"- `{dup_col}` → `t1_{dup_col}` (テーブル1), `t2_{dup_col}` (テーブル2)")
    else:
        # カラム未選択の場合は全カラムでの重複警告（結合キー除外後）
        if duplicate_cols_excluding_join_keys:
            st.warning(f"⚠️ 重複カラム検出: {len(duplicate_cols_excluding_join_keys)}個のカラム（結合キー除外後）に別名が付与されます。")
            with st.expander("🔍 重複カラム詳細", expanded=False):
                for dup_col in sorted(duplicate_cols_excluding_join_keys):
                    st.write(f"- `{dup_col}` → 各テーブル毎に別名付与")

else:
    st.info("🔸 テーブル選択と結合条件を設定するとカラム選択が可能になります")

# =========================================================
# STEP 4: WHERE条件・ソート・GROUP BY設定
# =========================================================
st.markdown("---")
st.markdown("### ⚙️ STEP 4: WHERE条件・ソート・GROUP BY設定")

if st.session_state.selected_table1 and st.session_state.selected_table2:
    # STEP 3で選択されたカラムがある場合はそれを基に絞り込み
    if st.session_state.adhoc_selected_columns:
        # 選択されたカラムから利用可能なカラム情報を構築
        all_columns = []
        
        # テーブル1のカラム
        table1_cols = get_table_columns(st.session_state.selected_table1)
        for col in table1_cols:
            # 選択されたカラムに含まれているかチェック
            t1_sql_name = f"t1.{quote_identifier(col['name'])}"
            if any(t1_sql_name in selected_col for selected_col in st.session_state.adhoc_selected_columns):
                all_columns.append({
                    'name': col['name'],
                    'type': col['type'],
                    'table': st.session_state.selected_table1,
                    'qualified_name': f"{st.session_state.selected_table1}.{col['name']}"
                })
        
        # テーブル2のカラム
        table2_cols = get_table_columns(st.session_state.selected_table2)
        for col in table2_cols:
            # 選択されたカラムに含まれているかチェック
            t2_sql_name = f"t2.{quote_identifier(col['name'])}"
            if any(t2_sql_name in selected_col for selected_col in st.session_state.adhoc_selected_columns):
                all_columns.append({
                    'name': col['name'],
                    'type': col['type'],
                    'table': st.session_state.selected_table2,
                    'qualified_name': f"{st.session_state.selected_table2}.{col['name']}"
                })
        
        # テーブル3のカラム（3テーブル結合の場合）
        if st.session_state.enable_3table_join and st.session_state.selected_table3:
            table3_cols = get_table_columns(st.session_state.selected_table3)
            for col in table3_cols:
                # 選択されたカラムに含まれているかチェック
                t3_sql_name = f"t3.{quote_identifier(col['name'])}"
                if any(t3_sql_name in selected_col for selected_col in st.session_state.adhoc_selected_columns):
                    all_columns.append({
                        'name': col['name'],
                        'type': col['type'],
                        'table': st.session_state.selected_table3,
                        'qualified_name': f"{st.session_state.selected_table3}.{col['name']}"
                    })
    else:
        # カラム未選択の場合は全カラムを表示（従来通り）
        all_columns = []
        
        # テーブル1のカラム
        table1_cols = get_table_columns(st.session_state.selected_table1)
        for col in table1_cols:
            all_columns.append({
                'name': col['name'],
                'type': col['type'],
                'table': st.session_state.selected_table1,
                'qualified_name': f"{st.session_state.selected_table1}.{col['name']}"
            })
        
        # テーブル2のカラム
        table2_cols = get_table_columns(st.session_state.selected_table2)
        for col in table2_cols:
            all_columns.append({
                'name': col['name'],
                'type': col['type'],
                'table': st.session_state.selected_table2,
                'qualified_name': f"{st.session_state.selected_table2}.{col['name']}"
            })
        
        # テーブル3のカラム（3テーブル結合の場合）
        if st.session_state.enable_3table_join and st.session_state.selected_table3:
            table3_cols = get_table_columns(st.session_state.selected_table3)
            for col in table3_cols:
                all_columns.append({
                    'name': col['name'],
                    'type': col['type'],
                    'table': st.session_state.selected_table3,
                    'qualified_name': f"{st.session_state.selected_table3}.{col['name']}"
                })
    
    col_left, col_right = st.columns(2)
    
    with col_left:
        # WHERE条件設定
        st.markdown("#### 🔍 WHERE条件")
        
        # 既存の条件の表示
        for i, condition in enumerate(st.session_state.adhoc_where_conditions_list):
            op = "WHERE" if i == 0 else condition['logic_op']
            quoted_col = quote_identifier(condition['column'])
            st.write(f"**{op.upper()}** `{quoted_col}` {condition['operator']} `'{condition['value']}'`")
            if st.button("🗑️", key=f"del_where_cond_{i}"):
                del st.session_state.adhoc_where_conditions_list[i]
                st.rerun()

        # 新しい条件の追加フォーム
        with st.expander("➕ WHERE条件を追加"):
            where_logic_op = st.selectbox("論理演算子", ["AND", "OR"], key="where_logic_op", disabled=(len(st.session_state.adhoc_where_conditions_list) == 0))
            
            # カラム選択（テーブル名付き）
            column_options = [""] + [f"{col['qualified_name']} ({col['type']})" for col in all_columns]
            selected_where_col = st.selectbox("カラムを選択", column_options, key="where_col_name")
            where_operator = st.selectbox("演算子を選択", ["=", ">", "<", ">=", "<=", "<>", "LIKE", "IN", "IS NULL", "IS NOT NULL"], key="where_operator")
            
            # 値の入力（演算子によって表示を変える）
            if where_operator in ["IS NULL", "IS NOT NULL"]:
                where_value = ""
                st.info(f"演算子 '{where_operator}' では値の入力は不要です")
            elif where_operator == "IN":
                where_value = st.text_input("値を入力（カンマ区切り）", key="where_value", placeholder="例: 'A','B','C'")
            elif where_operator == "LIKE":
                where_value = st.text_input("値を入力", key="where_value", placeholder="例: 東京 (自動で%東京%になります) または %東京 (手動指定)")
            else:
                where_value = st.text_input("値を入力", key="where_value")
            
            if st.button("追加", key="add_where_condition_btn") and selected_where_col:
                # カラム名を抽出（テーブル名.カラム名）
                col_name = selected_where_col.split(" (")[0]  # "(型)" を除去
                
                if where_operator in ["IS NULL", "IS NOT NULL"] or where_value:
                    st.session_state.adhoc_where_conditions_list.append({
                        "logic_op": where_logic_op,
                        "column": col_name,
                        "operator": where_operator,
                        "value": where_value
                    })
                    st.success("WHERE条件を追加しました！")
                    st.rerun()
                elif not where_value:
                    st.error("値を入力してください")
        
        # GROUP BY設定
        st.markdown("#### 📊 GROUP BY集計")
        
        # 既存のGROUP BY条件の表示
        for i, condition in enumerate(st.session_state.adhoc_group_by_conditions_list):
            # 新しいデータ構造と古いデータ構造の両方に対応
            if 'group_column' in condition:
                # 新しいデータ構造
                group_col = condition['group_column']
                agg_func = condition['aggregate_func']
                agg_col = condition['aggregate_column']
                st.write(f"**GROUP BY** `{group_col}` **集計**: {agg_func}(`{agg_col}`)")
            else:
                # 古いデータ構造（互換性のため）
                quoted_col = quote_identifier(condition['column'])
                if condition.get('aggregate_func'):
                    st.write(f"**GROUP BY** `{quoted_col}` **集計関数**: {condition['aggregate_func']}")
                else:
                    st.write(f"**GROUP BY** `{quoted_col}`")
            
            if st.button("🗑️", key=f"del_group_by_{i}"):
                del st.session_state.adhoc_group_by_conditions_list[i]
                st.rerun()
        
        # GROUP BYカラム追加（グルーピング用）
        st.markdown("##### ➕ グルーピングカラム追加")
        with st.expander("グルーピング対象カラムを追加"):
            group_column_options = [""] + [f"{col['qualified_name']} ({col['type']})" for col in all_columns]
            selected_group_col = st.selectbox("GROUP BYカラム", group_column_options, key="add_group_col", 
                                            help="グルーピングの単位となるカラム（例：性別、年収区分）")
            
            if st.button("追加", key="add_group_col_btn") and selected_group_col:
                # カラム名を抽出
                group_col_name = selected_group_col.split(" (")[0]
                
                # グルーピングカラムとして追加（集計関数なし）
                st.session_state.adhoc_group_by_conditions_list.append({
                    "group_column": group_col_name,
                    "aggregate_func": None,
                    "aggregate_column": None,
                    "is_grouping_column": True  # グルーピング専用カラムのフラグ
                })
                st.success(f"グルーピングカラム `{group_col_name}` を追加しました！")
                st.rerun()
        
        # 集計関数追加
        st.markdown("##### ➕ 集計関数追加")
        with st.expander("集計対象を追加"):
            # 集計関数の選択
            aggregate_functions = ["COUNT", "SUM", "AVG", "MAX", "MIN", "COUNT_DISTINCT"]
            selected_aggregate = st.selectbox("集計関数", aggregate_functions, key="add_aggregate_func",
                                            help="COUNT: 件数、SUM: 合計、AVG: 平均、MAX: 最大、MIN: 最小")
            
            # 集計対象カラムの選択
            if selected_aggregate == "COUNT":
                # COUNTの場合は特別扱い（任意のカラムまたは*）
                count_options = ["*（全行数）"] + [f"{col['qualified_name']} ({col['type']})" for col in all_columns]
                selected_agg_col = st.selectbox("COUNT対象", count_options, key="add_count_target_col")
                if selected_agg_col == "*（全行数）":
                    agg_col_name = "*"
                else:
                    agg_col_name = selected_agg_col.split(" (")[0]
            elif selected_aggregate == "COUNT_DISTINCT":
                # COUNT DISTINCTの場合
                agg_column_options = [""] + [f"{col['qualified_name']} ({col['type']})" for col in all_columns]
                selected_agg_col = st.selectbox("COUNT DISTINCT対象カラム", agg_column_options, key="add_count_distinct_col")
                if selected_agg_col:
                    agg_col_name = selected_agg_col.split(" (")[0]
                else:
                    agg_col_name = ""
            else:
                # SUM、AVG、MAX、MINの場合は数値型カラムのみ
                numeric_columns = [col for col in all_columns if is_numeric_type(col['type'])]
                if numeric_columns:
                    numeric_options = [""] + [f"{col['qualified_name']} ({col['type']})" for col in numeric_columns]
                    selected_agg_col = st.selectbox(f"{selected_aggregate}対象カラム（数値型）", numeric_options, key="add_numeric_agg_col")
                    if selected_agg_col:
                        agg_col_name = selected_agg_col.split(" (")[0]
                    else:
                        agg_col_name = ""
                else:
                    st.warning("数値型カラムがありません。COUNTまたはCOUNT_DISTINCTを選択してください。")
                    agg_col_name = ""
            
            # プレビュー表示
            if agg_col_name:
                if selected_aggregate == "COUNT_DISTINCT":
                    alias_name = f"count_distinct_{agg_col_name.replace('.', '_')}"
                    preview_text = f"{selected_aggregate}({quote_identifier(agg_col_name)}) AS {quote_identifier(alias_name)}"
                else:
                    alias_name = f"{selected_aggregate.lower()}_{agg_col_name.replace('.', '_').replace('*', 'all')}"
                    agg_col_display = agg_col_name if agg_col_name == "*" else quote_identifier(agg_col_name)
                    preview_text = f"{selected_aggregate}({agg_col_display}) AS {quote_identifier(alias_name)}"
                st.code(preview_text, language="sql")
            
            if st.button("追加", key="add_aggregate_btn") and agg_col_name:
                st.session_state.adhoc_group_by_conditions_list.append({
                    "group_column": None,
                    "aggregate_func": selected_aggregate,
                    "aggregate_column": agg_col_name,
                    "is_grouping_column": False  # 集計関数のフラグ
                })
                st.success(f"集計関数 {selected_aggregate}(`{agg_col_name}`) を追加しました！")
                st.rerun()

    
    with col_right:
        # ORDER BY設定
        st.markdown("#### 📈 ORDER BY（ソート条件）")
        
        # 既存のソート条件の表示
        for i, condition in enumerate(st.session_state.adhoc_order_by_conditions_list):
            quoted_col = quote_identifier(condition['column'])
            sort_type = condition.get('sort_type', '通常カラム')
            sort_type_icon = "🧮" if sort_type == "集計結果" else "📋"
            st.write(f"**ORDER BY** {sort_type_icon} `{quoted_col}` **{condition['direction']}** ({sort_type})")
            if st.button("🗑️", key=f"del_order_by_{i}"):
                del st.session_state.adhoc_order_by_conditions_list[i]
                st.rerun()

        # 新しいソート条件の追加フォーム
        with st.expander("➕ ORDER BY条件を追加"):
            # 基本カラムオプション
            sort_column_options = [""] + [f"{col['qualified_name']} ({col['type']})" for col in all_columns]
            
            # GROUP BYがある場合は集計関数のエイリアス名も追加
            if st.session_state.adhoc_group_by_conditions_list:
                st.markdown("**通常カラム**")
                selected_sort_col = st.selectbox("グルーピングカラムを選択", sort_column_options, key="sort_col_name")
                
                # 集計関数のエイリアス名オプション
                aggregate_options = [""]
                for condition in st.session_state.adhoc_group_by_conditions_list:
                    if condition.get('aggregate_func'):
                        agg_func = condition['aggregate_func']
                        agg_col = condition['aggregate_column']
                        
                        if agg_func == "COUNT_DISTINCT":
                            alias_name = f"count_distinct_{agg_col.replace('.', '_')}"
                        else:
                            alias_suffix = agg_col.replace('.', '_').replace('*', 'all')
                            alias_name = f"{agg_func.lower()}_{alias_suffix}"
                        
                        aggregate_options.append(f"🧮 {alias_name} ({agg_func})")
                
                if len(aggregate_options) > 1:
                    st.markdown("**集計結果**")
                    selected_aggregate_sort = st.selectbox("集計結果でソート", aggregate_options, key="sort_aggregate_col",
                                                         help="例: sum_利用明細_利用金額 で利用金額の多い順にソート")
                else:
                    selected_aggregate_sort = ""
                
                # どちらが選択されているかを判定
                if selected_aggregate_sort and selected_aggregate_sort != "":
                    # 集計関数のエイリアス名を抽出
                    alias_name = selected_aggregate_sort.split(" ")[1]  # "🧮 alias_name (func)" から alias_name を取得
                    final_sort_col = alias_name
                    sort_type = "集計結果"
                elif selected_sort_col and selected_sort_col != "":
                    # 通常カラム
                    final_sort_col = selected_sort_col.split(" (")[0]
                    sort_type = "通常カラム"
                else:
                    final_sort_col = ""
                    sort_type = ""
            else:
                # GROUP BYがない場合は通常の選択
                selected_sort_col = st.selectbox("ソート対象カラムを選択", sort_column_options, key="sort_col_name")
                if selected_sort_col:
                    final_sort_col = selected_sort_col.split(" (")[0]
                    sort_type = "通常カラム"
                else:
                    final_sort_col = ""
                    sort_type = ""
            
            sort_direction = st.selectbox("ソート方向を選択", ["ASC", "DESC"], key="sort_direction", help="ASC: 昇順（小→大）、DESC: 降順（大→小）")
            
            # プレビュー表示
            if final_sort_col:
                if sort_type == "集計結果":
                    preview_text = f"ORDER BY {quote_identifier(final_sort_col)} {sort_direction}  -- 集計結果でソート"
                else:
                    preview_text = f"ORDER BY {final_sort_col} {sort_direction}  -- 通常カラムでソート"
                st.code(preview_text, language="sql")
            
            if st.button("追加", key="add_order_by_btn") and final_sort_col:
                st.session_state.adhoc_order_by_conditions_list.append({
                    "column": final_sort_col,
                    "direction": sort_direction,
                    "sort_type": sort_type  # ソートタイプを記録
                })
                st.success(f"ORDER BY条件を追加しました！ ({sort_type}: {final_sort_col})")
                st.rerun()
        
        # 条件のクリア
        st.markdown("---")
        if st.button("🧹 すべての条件をクリア", key="clear_all_conditions"):
            st.session_state.adhoc_where_conditions_list = []
            st.session_state.adhoc_order_by_conditions_list = []
            st.session_state.adhoc_group_by_conditions_list = []
            st.success("すべての条件をクリアしました！")
            st.rerun()
else:
    st.info("🔸 STEP 1-3: テーブル選択と結合設定、カラム選択を完了してください")

# =========================================================
# STEP 5: SQLプレビュー・実行・結果表示
# =========================================================
st.markdown("---")
st.markdown("### 📝 STEP 5: SQLプレビュー・実行")

# 2テーブル結合モード
if not st.session_state.enable_3table_join and st.session_state.selected_table1 and st.session_state.selected_table2 and st.session_state.join_key1 and st.session_state.join_key2:
    
    # 2テーブルSQL生成（スキーマ名を含める）
    table1_schema = get_table_schema(st.session_state.selected_table1)
    table2_schema = get_table_schema(st.session_state.selected_table2)
    quoted_table1 = f"{table1_schema}.{quote_identifier(st.session_state.selected_table1)}"
    quoted_table2 = f"{table2_schema}.{quote_identifier(st.session_state.selected_table2)}"
    quoted_key1 = quote_identifier(st.session_state.join_key1)
    quoted_key2 = quote_identifier(st.session_state.join_key2)
    
    # 重複カラム名を検出（選択されたカラムのみを対象）
    cols1 = get_table_columns(st.session_state.selected_table1)
    cols2 = get_table_columns(st.session_state.selected_table2)
    
    if st.session_state.adhoc_selected_columns:
        # 選択されたカラムから実際に使用されているカラム名を抽出
        selected_col_names_t1 = set()
        selected_col_names_t2 = set()
        
        for selected_col in st.session_state.adhoc_selected_columns:
            # "t1.\"カラム名\"" または "t2.\"カラム名\" AS alias" の形式から元のカラム名を抽出
            if selected_col.startswith('t1.'):
                # t1.\"カラム名\" から カラム名 を抽出
                col_part = selected_col[3:]  # "t1." を除去
                if ' AS ' in col_part:
                    col_part = col_part.split(' AS ')[0]  # AS句があれば除去
                col_name = col_part.strip('"')  # クォートを除去
                selected_col_names_t1.add(col_name)
            elif selected_col.startswith('t2.'):
                # t2.\"カラム名\" から カラム名 を抽出
                col_part = selected_col[3:]  # "t2." を除去
                if ' AS ' in col_part:
                    col_part = col_part.split(' AS ')[0]  # AS句があれば除去
                col_name = col_part.strip('"')  # クォートを除去
                selected_col_names_t2.add(col_name)
        
        # 選択されたカラム間での重複をチェック
        duplicate_cols = selected_col_names_t1 & selected_col_names_t2
    else:
        # カラム未選択の場合は全カラムで重複チェック（従来通り）
        table1_col_names = {c['name'] for c in cols1}
        table2_col_names = {c['name'] for c in cols2}
        duplicate_cols = table1_col_names & table2_col_names
    
    # SELECT句を構築（選択されたカラムのみ）
    if st.session_state.adhoc_selected_columns:
        select_clause = "SELECT " + ",\n       ".join(sorted(st.session_state.adhoc_selected_columns))
    else:
        # カラム未選択の場合は全カラム（従来通り）
        select_parts = []
        
        # テーブル1のカラム
        for col in cols1:
            quoted_col = quote_identifier(col['name'])
            if col['name'] in duplicate_cols:
                alias_name = f"t1_{col['name']}"
                select_parts.append(f"t1.{quoted_col} AS {quote_identifier(alias_name)}")
            else:
                select_parts.append(f"t1.{quoted_col}")
        
        # テーブル2のカラム
        for col in cols2:
            quoted_col = quote_identifier(col['name'])
            if col['name'] in duplicate_cols:
                alias_name = f"t2_{col['name']}"
                select_parts.append(f"t2.{quoted_col} AS {quote_identifier(alias_name)}")
            else:
                select_parts.append(f"t2.{quoted_col}")
        
        select_clause = "SELECT " + ",\n       ".join(select_parts)
    
    # FROM句とJOIN句（2テーブル）
    join_query = f"""{select_clause}
FROM {quoted_table1} t1
{st.session_state.join_type1} {quoted_table2} t2 
ON t1.{quoted_key1} = t2.{quoted_key2}"""
    
    # WHERE句の追加
    if st.session_state.adhoc_where_conditions_list:
        where_clauses = []
        for i, condition in enumerate(st.session_state.adhoc_where_conditions_list):
            # テーブル名.カラム名の形式でエイリアスを考慮
            col_with_alias = condition['column']  # 例: "テーブル1.カラム名"
            if '.' in col_with_alias:
                table_name, col_name = col_with_alias.split('.', 1)
                # テーブル名をエイリアスに変換
                if table_name == st.session_state.selected_table1:
                    alias_col = f"t1.{quote_identifier(col_name)}"
                elif table_name == st.session_state.selected_table2:
                    alias_col = f"t2.{quote_identifier(col_name)}"
                else:
                    alias_col = quote_identifier(col_with_alias)
            else:
                alias_col = quote_identifier(col_with_alias)
            
            if condition['operator'] in ["IS NULL", "IS NOT NULL"]:
                cond_str = f"{alias_col} {condition['operator']}"
            elif condition['operator'] == "LIKE":
                # ユーザーが手動で%を指定している場合はそのまま使用、そうでなければ自動で%を付与
                like_value = condition['value']
                if not like_value.startswith('%') and not like_value.endswith('%'):
                    like_value = f"%{like_value}%"
                cond_str = f"{alias_col} LIKE '{like_value}'"
            elif condition['operator'] == "IN":
                cond_str = f"{alias_col} IN ({condition['value']})"
            else:
                cond_str = f"{alias_col} {condition['operator']} '{condition['value']}'"
            
            if i == 0:
                where_clauses.append(cond_str)
            else:
                where_clauses.append(f"{condition['logic_op']} {cond_str}")
        
        join_query += f"\nWHERE {' '.join(where_clauses)}"
    
    # GROUP BY句の追加
    if st.session_state.adhoc_group_by_conditions_list:
        group_by_columns = []
        aggregate_columns = []
        
        for condition in st.session_state.adhoc_group_by_conditions_list:
            # 新しいデータ構造と古いデータ構造の両方に対応
            if 'group_column' in condition:
                # 新しいデータ構造
                if condition.get('is_grouping_column', False):
                    # グルーピングカラムの場合
                    group_col_with_alias = condition['group_column']
                    
                    # GROUP BYカラムのエイリアス処理
                    if '.' in group_col_with_alias:
                        table_name, col_name = group_col_with_alias.split('.', 1)
                        if table_name == st.session_state.selected_table1:
                            group_alias_col = f"t1.{quote_identifier(col_name)}"
                        elif table_name == st.session_state.selected_table2:
                            group_alias_col = f"t2.{quote_identifier(col_name)}"
                        else:
                            group_alias_col = quote_identifier(group_col_with_alias)
                    else:
                        group_alias_col = quote_identifier(group_col_with_alias)
                    
                    group_by_columns.append(group_alias_col)
                
                elif condition.get('aggregate_func'):
                    # 集計関数の場合
                    agg_func = condition['aggregate_func']
                    agg_col_with_alias = condition['aggregate_column']
                    
                    # 集計カラムのエイリアス処理
                    if agg_col_with_alias == "*":
                        agg_alias_col = "*"
                    elif '.' in agg_col_with_alias:
                        table_name, col_name = agg_col_with_alias.split('.', 1)
                        if table_name == st.session_state.selected_table1:
                            agg_alias_col = f"t1.{quote_identifier(col_name)}"
                        elif table_name == st.session_state.selected_table2:
                            agg_alias_col = f"t2.{quote_identifier(col_name)}"
                        else:
                            agg_alias_col = quote_identifier(agg_col_with_alias)
                    else:
                        agg_alias_col = quote_identifier(agg_col_with_alias)
                    
                    # 集計関数を適用
                    if agg_func == "COUNT_DISTINCT":
                        alias_name = f"count_distinct_{agg_col_with_alias.replace('.', '_')}"
                        agg_expression = f"COUNT(DISTINCT {agg_alias_col}) AS {quote_identifier(alias_name)}"
                    else:
                        alias_suffix = agg_col_with_alias.replace('.', '_').replace('*', 'all')
                        alias_name = f"{agg_func.lower()}_{alias_suffix}"
                        agg_expression = f"{agg_func}({agg_alias_col}) AS {quote_identifier(alias_name)}"
                    
                    aggregate_columns.append(agg_expression)
            else:
                # 古いデータ構造（互換性のため）
                col_with_alias = condition['column']
                if '.' in col_with_alias:
                    table_name, col_name = col_with_alias.split('.', 1)
                    if table_name == st.session_state.selected_table1:
                        alias_col = f"t1.{quote_identifier(col_name)}"
                    elif table_name == st.session_state.selected_table2:
                        alias_col = f"t2.{quote_identifier(col_name)}"
                    else:
                        alias_col = quote_identifier(col_with_alias)
                else:
                    alias_col = quote_identifier(col_with_alias)
                group_by_columns.append(alias_col)
        
        # GROUP BYとSELECT句の修正
        if group_by_columns or aggregate_columns:
            # GROUP BY句がある場合、SELECT句を再構成
            select_clause = f"SELECT {', '.join(group_by_columns + aggregate_columns)}"
            # 元のSELECT句を置き換え（複数行にわたる場合も考慮）
            lines = join_query.split('\n')
            # SELECT句が複数行にわたる場合を考慮してFROMまでを置き換え
            from_index = -1
            for i, line in enumerate(lines):
                if line.strip().startswith('FROM'):
                    from_index = i
                    break
            
            if from_index > 0:
                # SELECT句部分を置き換え
                join_query = select_clause + '\n' + '\n'.join(lines[from_index:])
            else:
                # FROMが見つからない場合は最初の行のみ置き換え
                join_query = join_query.replace(lines[0], select_clause)
        
        if group_by_columns:
            join_query += f"\nGROUP BY {', '.join(group_by_columns)}"
    
    # ORDER BY句の追加（GROUP BY対応・集計結果ソート対応）
    if st.session_state.adhoc_order_by_conditions_list:
        order_by_clauses = []
        for condition in st.session_state.adhoc_order_by_conditions_list:
            col_with_alias = condition['column']
            sort_type = condition.get('sort_type', '通常カラム')
            
            if sort_type == "集計結果":
                # 集計結果でのソート（エイリアス名を直接使用）
                order_by_clauses.append(f"{quote_identifier(col_with_alias)} {condition['direction']}")
            elif st.session_state.adhoc_group_by_conditions_list:
                # GROUP BYがある場合のグルーピングカラム
                found_in_select = False
                
                # GROUP BYカラムにあるかチェック
                for group_condition in st.session_state.adhoc_group_by_conditions_list:
                    if group_condition.get('is_grouping_column', False) and group_condition.get('group_column') == col_with_alias:
                        # GROUP BYカラムの場合
                        if '.' in col_with_alias:
                            table_name, col_name = col_with_alias.split('.', 1)
                            if table_name == st.session_state.selected_table1:
                                alias_col = f"t1.{quote_identifier(col_name)}"
                            elif table_name == st.session_state.selected_table2:
                                alias_col = f"t2.{quote_identifier(col_name)}"
                            else:
                                alias_col = quote_identifier(col_with_alias)
                        else:
                            alias_col = quote_identifier(col_with_alias)
                        order_by_clauses.append(f"{alias_col} {condition['direction']}")
                        found_in_select = True
                        break
                
                if not found_in_select:
                    # 集計結果でのソートが利用可能になったため、この警告は不要
                    # ユーザーには集計結果でのソート機能を案内
                    st.info(f"💡 '{col_with_alias}' は通常カラムです。集計結果でソートしたい場合は「集計結果でソート」オプションをご利用ください。")
                    continue
            else:
                # GROUP BYがない場合は通常処理
                if '.' in col_with_alias:
                    table_name, col_name = col_with_alias.split('.', 1)
                    # テーブル名をエイリアスに変換
                    if table_name == st.session_state.selected_table1:
                        alias_col = f"t1.{quote_identifier(col_name)}"
                    elif table_name == st.session_state.selected_table2:
                        alias_col = f"t2.{quote_identifier(col_name)}"
                    else:
                        alias_col = quote_identifier(col_with_alias)
                else:
                    alias_col = quote_identifier(col_with_alias)
                order_by_clauses.append(f"{alias_col} {condition['direction']}")
        
        if order_by_clauses:
            join_query += f"\nORDER BY {', '.join(order_by_clauses)}"
    
    # 重複カラム情報を表示
    if duplicate_cols:
        st.warning(f"⚠️ 重複カラム検出: {len(duplicate_cols)}個のカラムが両テーブルに存在します。")
        with st.expander("🔍 重複カラム詳細", expanded=False):
            st.write("**重複カラム一覧:**")
            for dup_col in sorted(duplicate_cols):
                st.write(f"- `{dup_col}` → `t1_{dup_col}` (テーブル1), `t2_{dup_col}` (テーブル2)")

# 3テーブル結合モード
elif (st.session_state.enable_3table_join and st.session_state.selected_table1 and st.session_state.selected_table2 and st.session_state.selected_table3 and 
      st.session_state.join_key1 and st.session_state.join_key2 and st.session_state.join_key3 and st.session_state.join_key2_for_join2):
    
    # 3テーブルSQL生成（スキーマ名を含める）
    table1_schema = get_table_schema(st.session_state.selected_table1)
    table2_schema = get_table_schema(st.session_state.selected_table2)
    table3_schema = get_table_schema(st.session_state.selected_table3)
    quoted_table1 = f"{table1_schema}.{quote_identifier(st.session_state.selected_table1)}"
    quoted_table2 = f"{table2_schema}.{quote_identifier(st.session_state.selected_table2)}"
    quoted_table3 = f"{table3_schema}.{quote_identifier(st.session_state.selected_table3)}"
    quoted_key1 = quote_identifier(st.session_state.join_key1)
    quoted_key2 = quote_identifier(st.session_state.join_key2)
    quoted_key3 = quote_identifier(st.session_state.join_key3)
    
    # 重複カラム名を検出（3テーブル）
    cols1 = get_table_columns(st.session_state.selected_table1)
    cols2 = get_table_columns(st.session_state.selected_table2)
    cols3 = get_table_columns(st.session_state.selected_table3)
    
    table1_col_names = {c['name'] for c in cols1}
    table2_col_names = {c['name'] for c in cols2}
    table3_col_names = {c['name'] for c in cols3}
    
    # 全体での重複を検出
    all_cols_combined = table1_col_names | table2_col_names | table3_col_names
    duplicate_cols = set()
    
    for col_name in all_cols_combined:
        tables_with_col = []
        if col_name in table1_col_names:
            tables_with_col.append("t1")
        if col_name in table2_col_names:
            tables_with_col.append("t2")
        if col_name in table3_col_names:
            tables_with_col.append("t3")
        
        if len(tables_with_col) > 1:
            duplicate_cols.add(col_name)
    
    # SELECT句を構築（3テーブル・選択されたカラムのみ）
    if st.session_state.adhoc_selected_columns:
        select_clause = "SELECT " + ",\n       ".join(sorted(st.session_state.adhoc_selected_columns))
    else:
        # カラム未選択の場合は全カラム（従来通り）
        select_parts = []
        
        # テーブル1のカラム
        for col in cols1:
            quoted_col = quote_identifier(col['name'])
            if col['name'] in duplicate_cols:
                alias_name = f"t1_{col['name']}"
                select_parts.append(f"t1.{quoted_col} AS {quote_identifier(alias_name)}")
            else:
                select_parts.append(f"t1.{quoted_col}")
        
        # テーブル2のカラム
        for col in cols2:
            quoted_col = quote_identifier(col['name'])
            if col['name'] in duplicate_cols:
                alias_name = f"t2_{col['name']}"
                select_parts.append(f"t2.{quoted_col} AS {quote_identifier(alias_name)}")
            else:
                select_parts.append(f"t2.{quoted_col}")
        
        # テーブル3のカラム
        for col in cols3:
            quoted_col = quote_identifier(col['name'])
            if col['name'] in duplicate_cols:
                alias_name = f"t3_{col['name']}"
                select_parts.append(f"t3.{quoted_col} AS {quote_identifier(alias_name)}")
            else:
                select_parts.append(f"t3.{quoted_col}")
        
        select_clause = "SELECT " + ",\n       ".join(select_parts)
    
    # FROM句とJOIN句（3テーブル）
    # テーブル2の結合キー2を取得
    key2_for_join2 = st.session_state.join_key2_for_join2 if st.session_state.join_key2_for_join2 else st.session_state.join_key2
    quoted_key2_for_join2 = quote_identifier(key2_for_join2)
    
    join_query = f"""{select_clause}
FROM {quoted_table1} t1
{st.session_state.join_type1} {quoted_table2} t2 
ON t1.{quoted_key1} = t2.{quoted_key2}
{st.session_state.join_type2} {quoted_table3} t3 
ON t2.{quoted_key2_for_join2} = t3.{quoted_key3}"""
    
    # WHERE句の追加（3テーブル）
    if st.session_state.adhoc_where_conditions_list:
        where_clauses = []
        for i, condition in enumerate(st.session_state.adhoc_where_conditions_list):
            # テーブル名.カラム名の形式でエイリアスを考慮（3テーブル）
            col_with_alias = condition['column']
            if '.' in col_with_alias:
                table_name, col_name = col_with_alias.split('.', 1)
                # テーブル名をエイリアスに変換
                if table_name == st.session_state.selected_table1:
                    alias_col = f"t1.{quote_identifier(col_name)}"
                elif table_name == st.session_state.selected_table2:
                    alias_col = f"t2.{quote_identifier(col_name)}"
                elif table_name == st.session_state.selected_table3:
                    alias_col = f"t3.{quote_identifier(col_name)}"
                else:
                    alias_col = quote_identifier(col_with_alias)
            else:
                alias_col = quote_identifier(col_with_alias)
            
            if condition['operator'] in ["IS NULL", "IS NOT NULL"]:
                cond_str = f"{alias_col} {condition['operator']}"
            elif condition['operator'] == "LIKE":
                # ユーザーが手動で%を指定している場合はそのまま使用、そうでなければ自動で%を付与
                like_value = condition['value']
                if not like_value.startswith('%') and not like_value.endswith('%'):
                    like_value = f"%{like_value}%"
                cond_str = f"{alias_col} LIKE '{like_value}'"
            elif condition['operator'] == "IN":
                cond_str = f"{alias_col} IN ({condition['value']})"
            else:
                cond_str = f"{alias_col} {condition['operator']} '{condition['value']}'"
            
            if i == 0:
                where_clauses.append(cond_str)
            else:
                where_clauses.append(f"{condition['logic_op']} {cond_str}")
        
        join_query += f"\nWHERE {' '.join(where_clauses)}"
    
    # GROUP BY句の追加（3テーブル）
    if st.session_state.adhoc_group_by_conditions_list:
        group_by_columns = []
        aggregate_columns = []
        
        for condition in st.session_state.adhoc_group_by_conditions_list:
            # 新しいデータ構造と古いデータ構造の両方に対応
            if 'group_column' in condition:
                # 新しいデータ構造
                group_col_with_alias = condition['group_column']
                agg_func = condition['aggregate_func']
                agg_col_with_alias = condition['aggregate_column']
                
                # GROUP BYカラムのエイリアス処理（3テーブル）
                if '.' in group_col_with_alias:
                    table_name, col_name = group_col_with_alias.split('.', 1)
                    if table_name == st.session_state.selected_table1:
                        group_alias_col = f"t1.{quote_identifier(col_name)}"
                    elif table_name == st.session_state.selected_table2:
                        group_alias_col = f"t2.{quote_identifier(col_name)}"
                    elif table_name == st.session_state.selected_table3:
                        group_alias_col = f"t3.{quote_identifier(col_name)}"
                    else:
                        group_alias_col = quote_identifier(group_col_with_alias)
                else:
                    group_alias_col = quote_identifier(group_col_with_alias)
                
                group_by_columns.append(group_alias_col)
                
                # 集計カラムのエイリアス処理（3テーブル）
                if agg_col_with_alias == "*":
                    agg_alias_col = "*"
                elif '.' in agg_col_with_alias:
                    table_name, col_name = agg_col_with_alias.split('.', 1)
                    if table_name == st.session_state.selected_table1:
                        agg_alias_col = f"t1.{quote_identifier(col_name)}"
                    elif table_name == st.session_state.selected_table2:
                        agg_alias_col = f"t2.{quote_identifier(col_name)}"
                    elif table_name == st.session_state.selected_table3:
                        agg_alias_col = f"t3.{quote_identifier(col_name)}"
                    else:
                        agg_alias_col = quote_identifier(agg_col_with_alias)
                else:
                    agg_alias_col = quote_identifier(agg_col_with_alias)
                
                # 集計関数を適用
                if agg_func == "COUNT_DISTINCT":
                    alias_name = f"count_distinct_{agg_col_with_alias.replace('.', '_')}"
                    agg_expression = f"COUNT(DISTINCT {agg_alias_col}) AS {quote_identifier(alias_name)}"
                else:
                    alias_suffix = agg_col_with_alias.replace('.', '_').replace('*', 'all')
                    alias_name = f"{agg_func.lower()}_{alias_suffix}"
                    agg_expression = f"{agg_func}({agg_alias_col}) AS {quote_identifier(alias_name)}"
                
                aggregate_columns.append(agg_expression)
            else:
                # 古いデータ構造（互換性のため）
                col_with_alias = condition['column']
                if '.' in col_with_alias:
                    table_name, col_name = col_with_alias.split('.', 1)
                    if table_name == st.session_state.selected_table1:
                        alias_col = f"t1.{quote_identifier(col_name)}"
                    elif table_name == st.session_state.selected_table2:
                        alias_col = f"t2.{quote_identifier(col_name)}"
                    elif table_name == st.session_state.selected_table3:
                        alias_col = f"t3.{quote_identifier(col_name)}"
                    else:
                        alias_col = quote_identifier(col_with_alias)
                else:
                    alias_col = quote_identifier(col_with_alias)
                group_by_columns.append(alias_col)
        
        # GROUP BYとSELECT句の修正（3テーブル）
        if aggregate_columns:
            # 集計関数がある場合、SELECT句を再構成
            select_clause = f"SELECT {', '.join(group_by_columns + aggregate_columns)}"
            # 元のSELECT句を置き換え（複数行にわたる場合も考慮）
            lines = join_query.split('\n')
            # SELECT句が複数行にわたる場合を考慮してFROMまでを置き換え
            from_index = -1
            for i, line in enumerate(lines):
                if line.strip().startswith('FROM'):
                    from_index = i
                    break
            
            if from_index > 0:
                # SELECT句部分を置き換え
                join_query = select_clause + '\n' + '\n'.join(lines[from_index:])
            else:
                # FROMが見つからない場合は最初の行のみ置き換え
                join_query = join_query.replace(lines[0], select_clause)
        
        join_query += f"\nGROUP BY {', '.join(group_by_columns)}"
    
    # ORDER BY句の追加（3テーブル）
    if st.session_state.adhoc_order_by_conditions_list:
        order_by_clauses = []
        for condition in st.session_state.adhoc_order_by_conditions_list:
            # テーブル名.カラム名の形式でエイリアスを考慮（3テーブル）
            col_with_alias = condition['column']
            if '.' in col_with_alias:
                table_name, col_name = col_with_alias.split('.', 1)
                # テーブル名をエイリアスに変換
                if table_name == st.session_state.selected_table1:
                    alias_col = f"t1.{quote_identifier(col_name)}"
                elif table_name == st.session_state.selected_table2:
                    alias_col = f"t2.{quote_identifier(col_name)}"
                elif table_name == st.session_state.selected_table3:
                    alias_col = f"t3.{quote_identifier(col_name)}"
                else:
                    alias_col = quote_identifier(col_with_alias)
            else:
                alias_col = quote_identifier(col_with_alias)
            order_by_clauses.append(f"{alias_col} {condition['direction']}")
        join_query += f"\nORDER BY {', '.join(order_by_clauses)}"
    
    # 重複カラム情報を表示（3テーブル）
    if duplicate_cols:
        st.warning(f"⚠️ 重複カラム検出: {len(duplicate_cols)}個のカラムが複数テーブルに存在します。")
        with st.expander("🔍 重複カラム詳細", expanded=False):
            st.write("**重複カラム一覧:**")
            for dup_col in sorted(duplicate_cols):
                tables_info = []
                if dup_col in table1_col_names:
                    tables_info.append(f"`t1_{dup_col}` (テーブル1)")
                if dup_col in table2_col_names:
                    tables_info.append(f"`t2_{dup_col}` (テーブル2)")
                if dup_col in table3_col_names:
                    tables_info.append(f"`t3_{dup_col}` (テーブル3)")
                st.write(f"- `{dup_col}` → {', '.join(tables_info)}")

# SQL実行部分（共通）
# カラム選択必須条件を追加
can_execute_2table = (not st.session_state.enable_3table_join and st.session_state.selected_table1 and st.session_state.selected_table2 and st.session_state.join_key1 and st.session_state.join_key2)
can_execute_3table = (st.session_state.enable_3table_join and st.session_state.selected_table1 and st.session_state.selected_table2 and st.session_state.selected_table3 and 
                     st.session_state.join_key1 and st.session_state.join_key2 and st.session_state.join_key3 and st.session_state.join_key2_for_join2)

if (can_execute_2table or can_execute_3table) and st.session_state.adhoc_selected_columns:
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("#### 📄 生成されたSQL")
        st.code(join_query, language="sql")
    
    with col2:
        limit_rows = st.number_input("表示件数制限", min_value=1, max_value=500, value=50, step=50, key="adhoc_limit")
        
        if st.button("🚀 クエリ実行", type="primary", key="execute_join_query"):
            execute_query(join_query, limit_rows)
            
            # タブ切り替えのフラグを設定
            st.session_state.active_tab = "📄 検索結果"
            st.session_state.show_result_from_main = True
        
        # 非定型検索オブジェクト保存
        st.markdown("---")
        st.markdown("#### 💾 検索条件保存")
        
        object_name = st.text_input("オブジェクト名", key="adhoc_object_name", placeholder="例: 会員利用明細結合")
        object_desc = st.text_area("説明", key="adhoc_object_desc", placeholder="この検索の用途を記述")
        
        if st.button("💾 保存", key="save_adhoc_object"):
            if object_name:
                import uuid
                
                object_data = {
                    'object_id': f"adhoc_{uuid.uuid4().hex[:12]}",
                    'object_name': object_name,
                    'description': object_desc,
                    'table1_name': st.session_state.selected_table1,
                    'table2_name': st.session_state.selected_table2,
                    'join_type': st.session_state.join_type1,
                    'join_key1': st.session_state.join_key1,
                    'join_key2': st.session_state.join_key2,
                    'search_query': join_query,
                    'is_favorite': False
                }
                if save_adhoc_search_object(object_data):
                    st.success("非定型検索オブジェクトを保存しました！")
                    st.rerun()
            else:
                st.warning("オブジェクト名を入力してください")

else:
    # 進行状況ガイド（カラム選択必須を追加）
    if st.session_state.enable_3table_join:
        if not st.session_state.selected_table1:
            st.info("🔸 STEP 1: テーブル1を選択してください")
        elif not st.session_state.selected_table2:
            st.info("🔸 STEP 1: テーブル2を選択してください")
        elif not st.session_state.selected_table3:
            st.info("🔸 STEP 1: テーブル3を選択してください")
        elif not st.session_state.join_key1 or not st.session_state.join_key2 or not st.session_state.join_key3 or not st.session_state.join_key2_for_join2:
            st.info("🔸 STEP 2: 全ての結合キーを設定してください")
        elif not st.session_state.adhoc_selected_columns:
            st.info("🔸 STEP 3: 出力するカラムを選択してください")
    else:
        if not st.session_state.selected_table1:
            st.info("🔸 STEP 1: テーブル1を選択してください")
        elif not st.session_state.selected_table2:
            st.info("🔸 STEP 1: テーブル2を選択してください")
        elif not st.session_state.join_key1 or not st.session_state.join_key2:
            st.info("🔸 STEP 2: 結合キーを設定してください")
        elif not st.session_state.adhoc_selected_columns:
            st.info("🔸 STEP 3: 出力するカラムを選択してください")

# =========================================================
# タブ追加: 検索結果・保存済み検索・スケジュール実行
# =========================================================
st.markdown("---")

# タブの選択状態を管理
tab_options = ["📄 検索結果", "📋 保存済み検索", "⏰ スケジュール実行", "⭐ お気に入り"]
if st.session_state.active_tab not in tab_options:
    st.session_state.active_tab = "📄 検索結果"

# アクティブタブのインデックスを取得
active_tab_index = tab_options.index(st.session_state.active_tab)

# タブを作成（selected_indexは使用できないため、別のアプローチを使用）
tab1, tab2, tab3, tab4 = st.tabs(tab_options)

with tab1:
    st.subheader("📄 出力結果")
    
    # 実行元に応じた特別表示
    if hasattr(st.session_state, 'show_result_from_saved') and st.session_state.show_result_from_saved:
        st.success("🔄 保存済み検索から実行された結果が表示されています")
        # フラグをリセット
        st.session_state.show_result_from_saved = False
    elif hasattr(st.session_state, 'show_result_from_main') and st.session_state.show_result_from_main:
        st.success("🚀 メイン検索から実行された結果が表示されています")
        # フラグをリセット
        st.session_state.show_result_from_main = False
    elif hasattr(st.session_state, 'show_result_from_work_table') and st.session_state.show_result_from_work_table:
        st.success("📦 WORK_テーブルから実行された結果が表示されています")
        # フラグをリセット
        st.session_state.show_result_from_work_table = False
    
    if st.session_state.search_result_df is not None:
        
        # 結果サマリー
        col_sum1, col_sum2, col_sum3 = st.columns(3)
        with col_sum1:
            st.metric("📊 取得行数", f"{len(st.session_state.search_result_df):,}行")
        with col_sum2:
            st.metric("📋 カラム数", f"{len(st.session_state.search_result_df.columns):,}列")
        with col_sum3:
            st.metric("💾 データサイズ", f"{st.session_state.search_result_df.memory_usage(deep=True).sum() / 1024:.1f} KB")
        
        st.dataframe(st.session_state.search_result_df, use_container_width=True, height=600)
        
        # ダウンロードと作業テーブル保存
        # タイムスタンプを共通で使用
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        col_dl1, col_dl2 = st.columns([1, 1])
        
        with col_dl1:
            # CSVダウンロード
            csv = st.session_state.search_result_df.to_csv(index=False)
            st.download_button(
                label="💾 CSVダウンロード", 
                data=csv, 
                file_name=f"adhoc_search_result_{timestamp}.csv", 
                mime="text/csv"
            )
        
        with col_dl2:
            # 作業テーブル保存機能
            save_as_work_table = st.selectbox(
                "📦 作業テーブルとして保存しますか？",
                ["いいえ", "はい"],
                key="save_work_table_choice",
                help="検索結果をSnowflake内の作業テーブルとして永続化します"
            )
            
            if save_as_work_table == "はい":
                # セッション状態でテーブル名を管理（デフォルト値の初期化）
                if 'work_table_default_name' not in st.session_state:
                    st.session_state.work_table_default_name = f"ADHOC_{timestamp}"
                
                work_table_name_input = st.text_input(
                    "作業テーブル名（WORK_は自動付与、必ず英語名で指定してください）",
                    value=st.session_state.work_table_default_name,
                    key="work_table_name",
                    help="WORK_接頭辞は自動で付与されます。入力したテーブル名がそのまま使用されます。",
                    placeholder="例: MY_SEARCH_RESULT"
                )
                
                # 最終的なテーブル名（必ずWORK_で始まる）
                if work_table_name_input:
                    # 入力値をトリムして空白を除去し、大文字に変換
                    cleaned_input = work_table_name_input.strip().upper()
                    
                    # 不正文字を除去（英数字とアンダースコアのみ許可）
                    import re
                    cleaned_input = re.sub(r'[^A-Z0-9_]', '_', cleaned_input)
                    
                    # WORK_が既に付いている場合はそのまま、そうでなければ追加
                    if cleaned_input.startswith("WORK_"):
                        final_table_name = cleaned_input
                    else:
                        final_table_name = f"WORK_{cleaned_input}"
                    
                    # スキーマ付きの完全なテーブル名
                    final_work_table_name = f"application_db.application_schema.{final_table_name}"
                    st.code(f"保存予定テーブル名: {final_work_table_name}", language="text")
                    
                    # 入力値の確認表示
                    st.info(f"🔍 入力されたテーブル名: `{work_table_name_input}` → 最終テーブル名: `{final_table_name}`")
                    
                    if st.button("💾 作業テーブル保存", key="save_work_table_btn", type="secondary"):
                        with st.spinner(f"作業テーブル `{final_table_name}` を保存中..."):
                            success, message = save_result_as_work_table(st.session_state.search_result_df, final_work_table_name)
                            if success:
                                # 行数を含めた成功メッセージを1つだけ表示
                                row_count = len(st.session_state.search_result_df)
                                st.success(f"✅ 作業テーブル `{final_table_name}` として正常に保存されました！（{row_count:,}行）")
                                
                                # 保存成功時にWORK_テーブル一覧も更新表示（改善版）
                                try:
                                    # スキーマを指定したWORK_テーブル検索
                                    work_tables = session.sql("SHOW TABLES LIKE 'WORK_%' IN application_db.application_schema").collect()
                                    st.info(f"📋 現在のWORK_テーブル: {len(work_tables)}個")
                                except Exception as e:
                                    # フォールバック: information_schema使用
                                    try:
                                        work_tables_query = """
                                        SELECT table_name 
                                        FROM information_schema.tables 
                                        WHERE table_schema = 'APPLICATION_SCHEMA' 
                                        AND table_name LIKE 'WORK_%'
                                        """
                                        work_tables = session.sql(work_tables_query).collect()
                                        st.info(f"📋 現在のWORK_テーブル: {len(work_tables)}個")
                                    except:
                                        st.info("📋 WORK_テーブル数の取得に失敗しました")
                            else:
                                st.error(f"❌ 保存エラー: {message}")
                else:
                    st.warning("作業テーブル名を入力してください")
    else:
        st.info("ここに最新の実行結果を表示します。上部でクエリを実行してください。")

with tab2:
    st.subheader("📋 保存済み非定型検索")
    
    # 検索機能のみ（トグルは削除）
    search_text = st.text_input("🔍 検索", key="search_adhoc_objects", placeholder="例: 会員利用、WORK_テーブル名")
    
    # =========================================================
    # 🔗 非定型検索オブジェクト（メインコンテンツ）
    # =========================================================
    st.markdown("#### 🔗 非定型検索オブジェクト")
    
    # テーブルはsetup SQLで事前作成済み
    adhoc_objects = load_adhoc_search_objects()
    
    # 検索フィルター適用
    if adhoc_objects:
        # テキスト検索フィルター
        if search_text:
            adhoc_objects = [obj for obj in adhoc_objects if search_text.lower() in obj['OBJECT_NAME'].lower()]
        
        if adhoc_objects:
            for i, obj in enumerate(adhoc_objects):
                # お気に入りアイコンの表示
                favorite_icon = "⭐" if obj.get('IS_FAVORITE', False) else "☆"
                expander_title = f"{favorite_icon} {obj['OBJECT_NAME']} ({obj['TABLE1_NAME']} ⇄ {obj['TABLE2_NAME']})"
                
                with st.expander(expander_title, expanded=False):
                    col1, col2 = st.columns([2, 3])
                    with col1:
                        st.write(f"**説明**: {obj['DESCRIPTION'] or '説明なし'}")
                        st.write(f"**結合構成**: {obj['TABLE1_NAME']} {obj['JOIN_TYPE']} {obj['TABLE2_NAME']}")
                        st.write(f"**結合キー**: {obj['JOIN_KEY1']} = {obj['JOIN_KEY2']}")
                        st.write(f"**作成日**: {obj['CREATED_AT']}")
                        st.write(f"**実行回数**: {obj['EXECUTION_COUNT']}")
                        if obj['LAST_EXECUTED']:
                            st.write(f"**最終実行**: {obj['LAST_EXECUTED']}")
                        
                        # 実行機能（実行回数カウント付き）
                        col_fav, col_exec = st.columns(2)
                        
                        with col_fav:
                            # お気に入り切り替えボタン
                            fav_label = "⭐ 解除" if obj.get('IS_FAVORITE', False) else "☆ お気に入り"
                            if st.button(fav_label, key=f"fav_adhoc_{i}", type="secondary"):
                                toggle_adhoc_favorite(obj['OBJECT_ID'])
                                st.success("お気に入り状態を更新しました！")
                                st.rerun()
                        
                        with col_exec:
                            exec_limit = st.number_input("件数", min_value=1, max_value=500, value=50, step=50, key=f"exec_limit_{i}")
                            if st.button("▶️ 実行", key=f"exec_adhoc_{i}", type="primary", help="この検索を直接実行"):
                                # 実行回数を更新
                                update_adhoc_execution_count(obj['OBJECT_ID'])
                                
                                # クエリを実行
                                execute_query(obj['SEARCH_QUERY'], exec_limit)
                                
                                # タブ切り替えのフラグを設定
                                st.session_state.active_tab = "📄 検索結果"
                                st.session_state.show_result_from_saved = True
                                
                                st.success(f"✅ 「{obj['OBJECT_NAME']}」を実行しました！")
                                st.info("📄 結果は「検索結果」タブに表示されています。")
                                
                                # JavaScriptでタブ切り替えを試行
                                st.components.v1.html("""
                                <script>
                                setTimeout(function() {
                                    // 検索結果タブ（最初のタブ）をクリック
                                    const tabs = document.querySelectorAll('[data-testid="stTabs"] button');
                                    if (tabs.length > 0) {
                                        tabs[0].click();
                                    }
                                }, 100);
                                </script>
                                """, height=0)
                                
                                st.rerun()
                    
                    with col2:
                        st.markdown("**💻 保存されたSQL**")
                        st.code(obj['SEARCH_QUERY'], language="sql")
                        
                        # 結合の詳細情報
                        st.markdown("**🔗 結合詳細**")
                        st.info(f"**{obj['JOIN_TYPE']}**: `{obj['TABLE1_NAME']}.{obj['JOIN_KEY1']}` = `{obj['TABLE2_NAME']}.{obj['JOIN_KEY2']}`")
        else:
            if search_text:
                st.info(f"「{search_text}」に該当する非定型検索オブジェクトがありません。")
            else:
                st.info("保存済みの非定型検索オブジェクトがありません。")
    else:
        st.info("保存済みの非定型検索オブジェクトがありません。")
    
    st.markdown("---")
    
    # =========================================================
    # 📦 作業テーブル一覧（サブコンテンツ）
    # =========================================================
    st.markdown("#### 📦 作業テーブル (WORK_始まり)")
    
    # シンプルなWORK_テーブル検索（LIKE句を使用）
    try:
        # スキーマを指定してWORK_テーブルを検索
        work_tables_result = session.sql("SHOW TABLES LIKE 'WORK_%' IN application_db.application_schema").collect()
        
        if work_tables_result:
            work_tables_info = []
            for table in work_tables_result:
                # テーブルの行数も取得
                try:
                    # スキーマ付きでテーブル名を指定
                    full_table_name = f"application_db.application_schema.{quote_identifier(table['name'])}"
                    count_result = session.sql(f"SELECT COUNT(*) FROM {full_table_name}").collect()
                    row_count = count_result[0][0] if count_result else 0
                except:
                    row_count = "取得エラー"
                
                work_tables_info.append({
                    'テーブル名': table['name'],
                    '作成日': table['created_on'],
                    '行数': f"{row_count:,}" if isinstance(row_count, int) else row_count
                })
                
            # 検索フィルター適用（WORK_テーブル用）
            if search_text:
                filtered_work_tables = [t for t in work_tables_info if search_text.lower() in t['テーブル名'].lower()]
                work_tables_info = filtered_work_tables
                # デバッグ情報
                if search_text and work_tables_info:
                    st.info(f"🔍 「{search_text}」でフィルター: {len(work_tables_info)}件のWORK_テーブルが見つかりました")
                elif search_text and not work_tables_info:
                    st.warning(f"🔍 「{search_text}」に該当するWORK_テーブルがありません")
                
            if work_tables_info:
                # WORK_テーブル一覧表示
                df_work_tables = pd.DataFrame(work_tables_info)
                st.dataframe(df_work_tables, use_container_width=True, hide_index=True)
                
                # サンプルデータプレビュー機能
                st.markdown("##### 📋 サンプルデータプレビュー")
                work_table_names = [t['テーブル名'] for t in work_tables_info]
                selected_work_table = st.selectbox(
                    "確認したいWORK_テーブルを選択",
                    [""] + work_table_names,
                    key="selected_work_table_preview",
                    help="選択したテーブルの先頭10行をプレビュー表示"
                )
                
                if selected_work_table:
                    try:
                        # スキーマ付きでテーブル名を指定
                        full_table_name = f"application_db.application_schema.{quote_identifier(selected_work_table)}"
                        preview_query = f"SELECT * FROM {full_table_name} LIMIT 10"
                        with st.spinner(f"サンプルデータ取得中..."):
                            preview_df = session.sql(preview_query).to_pandas()
                            
                            # プレビュー情報
                            col_prev1, col_prev2, col_prev3 = st.columns(3)
                            with col_prev1:
                                st.metric("📊 サンプル行数", f"{len(preview_df)}/10行")
                            with col_prev2:
                                st.metric("📋 総カラム数", f"{len(preview_df.columns)}列")
                            with col_prev3:
                                try:
                                    total_count_result = session.sql(f"SELECT COUNT(*) FROM {full_table_name}").collect()
                                    total_count = total_count_result[0][0]
                                    st.metric("📈 総行数", f"{total_count:,}行")
                                except:
                                    st.metric("📈 総行数", "取得エラー")
                            
                            # サンプルデータ表示
                            st.dataframe(preview_df, use_container_width=True, height=300)
                            
                            # クエリ実行オプション
                            st.markdown("##### 🚀 全データ表示")
                            full_limit = st.number_input("表示件数制限", min_value=1, max_value=500, value=50, step=50, key=f"work_table_full_limit")
                            
                            if st.button("📊 全データ表示", key=f"show_full_work_table", type="secondary"):
                                full_query = f"SELECT * FROM {full_table_name} LIMIT {full_limit}"
                                execute_query(full_query, full_limit)
                                
                                # タブ切り替えのフラグを設定
                                st.session_state.active_tab = "📄 検索結果"
                                st.session_state.show_result_from_work_table = True
                                st.success(f"✅ WORK_テーブル「{selected_work_table}」を表示しました！")
                                st.rerun()
                                
                    except Exception as e:
                        st.error(f"サンプルデータ取得エラー: {str(e)}")
                else:
                    st.info("検索条件に該当するWORK_テーブルがありません。")
        else:
            st.info("WORK_で始まる作業テーブルがありません。")
    except Exception as e:
        st.error(f"WORK_テーブル取得エラー: {str(e)}")
        
with tab3:
    st.subheader("⏰ スケジュール実行")
    
    # テーブルはsetup SQLで事前作成済み
    adhoc_objects = load_adhoc_search_objects()
    if adhoc_objects:
        object_options = {f"{obj['OBJECT_NAME']} ({obj['OBJECT_ID']})": obj['OBJECT_ID'] for obj in adhoc_objects}
        selected_object = st.selectbox("スケジュール対象を選択", list(object_options.keys()), key="schedule_object")
        
        # 選択されたオブジェクトの詳細情報を表示
        if selected_object:
            selected_obj = next(obj for obj in adhoc_objects if obj['OBJECT_ID'] == object_options[selected_object])
            table_info = f"{selected_obj['TABLE1_NAME']} ⇄ {selected_obj['TABLE2_NAME']}"
            
            st.info(f"**選択中**: {selected_obj['OBJECT_NAME']} ({table_info})")
            with st.expander("📋 SQL詳細", expanded=False):
                st.code(selected_obj['SEARCH_QUERY'], language="sql")
        
        col1, col2 = st.columns(2)
        with col1:
            schedule_type = st.selectbox("スケジュールタイプ", ["毎日", "毎週", "毎月", "カスタム"], key="adhoc_schedule_type") 
            
            # 30分単位の時刻選択肢を生成
            time_options = []
            for hour in range(24):
                for minute in [0, 30]:
                    time_str = f"{hour:02d}:{minute:02d}"
                    time_options.append(time_str)
            
            if schedule_type == "毎日":
                execution_time_str = st.selectbox("実行時刻", time_options, index=time_options.index("09:00"), key="adhoc_daily_time")
                hour, minute = map(int, execution_time_str.split(":"))
                cron_expression = f"{minute} {hour} * * *"
            elif schedule_type == "毎週":
                execution_time_str = st.selectbox("実行時刻", time_options, index=time_options.index("09:00"), key="adhoc_weekly_time")
                weekday = st.selectbox("曜日", ["月", "火", "水", "木", "金", "土", "日"], key="adhoc_weekday") 
                weekday_map = {"月": "1", "火": "2", "水": "3", "木": "4", "金": "5", "土": "6", "日": "0"}
                hour, minute = map(int, execution_time_str.split(":"))
                cron_expression = f"{minute} {hour} * * {weekday_map[weekday]}"
            elif schedule_type == "毎月":
                execution_time_str = st.selectbox("実行時刻", time_options, index=time_options.index("09:00"), key="adhoc_monthly_time")
                day_of_month = st.number_input("日", min_value=1, max_value=31, value=1, key="adhoc_day")
                hour, minute = map(int, execution_time_str.split(":"))
                cron_expression = f"{minute} {hour} {day_of_month} * *"
            else:
                cron_expression = st.text_input("Cron式", value="0 9 * * *", help="例: 0 9 * * * (毎日9時)", key="adhoc_cron")
        
        with col2:
            task_name = st.text_input("タスク名", value=f"adhoc_{object_options[selected_object][:8] if selected_object else 'new'}_{datetime.now().strftime('%Y%m%d')}", key="adhoc_task_name")
            task_description = st.text_area("タスク説明", key="adhoc_task_desc", placeholder="例: 毎朝9時に前日の会員別売上データを自動集計して作業テーブルに保存します")
            
            # 作業テーブル名の指定
            if selected_object:
                # WORK_SCHEDULED_接頭辞を自動付与（検索結果タブと同じ形式）
                base_name = f"{object_options[selected_object][:8]}_{datetime.now().strftime('%Y%m%d')}"
                
                work_table_name_input = st.text_input(
                    "作業テーブル名（WORK_SCHEDULED_は自動付与）、必ず英語名で指定してください",
                    value=base_name,
                    key="adhoc_work_table_name",
                    help="WORK_SCHEDULED_接頭辞は自動で付与されます"
                )
                
                # 最終的なテーブル名（必ずWORK_SCHEDULED_で始まる）
                work_table_name = f"application_db.application_schema.WORK_SCHEDULED_{work_table_name_input}" if not work_table_name_input.startswith("WORK_SCHEDULED_") else f"application_db.application_schema.{work_table_name_input}"
                st.code(f"保存予定テーブル名: {work_table_name}", language="text")
            
            # ユーザー向けの分かりやすい説明
            if schedule_type == "毎日":
                st.info(f"📅 **実行予定**: 毎日 {execution_time_str} (日本時間) に実行されます")
            elif schedule_type == "毎週":
                st.info(f"📅 **実行予定**: 毎週{weekday}曜日 {execution_time_str} (日本時間) に実行されます")
            elif schedule_type == "毎月":
                st.info(f"📅 **実行予定**: 毎月{day_of_month}日 {execution_time_str} (日本時間) に実行されます")
            else:
                st.info("📅 **実行予定**: カスタムCron式に基づいて実行されます (日本時間)")

            # Cron式のプレビューと説明
            st.caption(f"**Cron式**: `{cron_expression} Asia/Tokyo` *システム管理者向け")

        
        if st.button("⏰ スケジュール登録", type="primary", key="register_adhoc_schedule", disabled=not selected_object):
            if selected_object and work_table_name_input:
                selected_obj = next(obj for obj in adhoc_objects if obj['OBJECT_ID'] == object_options[selected_object])
                success, message = create_snowflake_task_for_adhoc(
                    task_name, 
                    cron_expression, 
                    selected_obj['SEARCH_QUERY'], 
                    work_table_name
                )
                if success:
                    st.success(f"スケジュールを登録しました: {message}")
                    st.info(f"💡 スケジュール実行結果は `{work_table_name}` テーブルに自動保存されます。")
                    st.rerun()
                else:
                    st.error(f"スケジュール登録エラー: {message}")
            else:
                st.warning("オブジェクトと作業テーブル名を指定してください")
        
        # 登録済みタスク一覧
        st.markdown("---")
        st.markdown("### 📅 登録済みスケジュール一覧")
        
        tasks = get_scheduled_tasks_adhoc()
        if tasks:
            for task in tasks:
                with st.expander(f"📋 {task['name']}", expanded=False):
                    col_task1, col_task2 = st.columns([2, 1])
                    with col_task1:
                        st.write(f"**状態**: {task['state']}")
                        st.write(f"**作成日**: {task.get('created_on', 'N/A')}")
                        
                        # スケジュール情報を分かりやすく表示
                        schedule_info = task.get('schedule', 'N/A')
                        if schedule_info != 'N/A' and 'USING CRON' in schedule_info:
                            # Cron式から分かりやすい説明を生成
                            try:
                                cron_part = schedule_info.split('USING CRON ')[1].split(' Asia/Tokyo')[0]
                                minute, hour, day, month, weekday = cron_part.split()
                                
                                if day == '*' and month == '*' and weekday == '*':
                                    # 毎日
                                    schedule_display = f"📅 **実行予定**: 毎日 {hour.zfill(2)}:{minute.zfill(2)} (日本時間) に実行されます"
                                elif day == '*' and month == '*' and weekday != '*':
                                    # 毎週
                                    weekday_names = {"0": "日", "1": "月", "2": "火", "3": "水", "4": "木", "5": "金", "6": "土"}
                                    weekday_name = weekday_names.get(weekday, weekday)
                                    schedule_display = f"📅 **実行予定**: 毎週{weekday_name}曜日 {hour.zfill(2)}:{minute.zfill(2)} (日本時間) に実行されます"
                                elif day != '*' and month == '*' and weekday == '*':
                                    # 毎月
                                    schedule_display = f"📅 **実行予定**: 毎月{day}日 {hour.zfill(2)}:{minute.zfill(2)} (日本時間) に実行されます"
                                else:
                                    # カスタム
                                    schedule_display = f"📅 **実行予定**: カスタムスケジュール ({cron_part})"
                            except:
                                schedule_display = f"**スケジュール**: {schedule_info}"
                        else:
                            schedule_display = f"**スケジュール**: {schedule_info}"
                        
                        st.write(schedule_display)
                    
                    with col_task2:
                        if task['state'] == 'started':
                            if st.button("⏸️ 一時停止", key=f"suspend_{task['name']}"):
                                success, msg = suspend_task_adhoc(task['name'])
                                if success:
                                    st.success(msg)
                                    st.rerun()
                                else:
                                    st.error(f"エラー: {msg}")
                        else:
                            if st.button("▶️ 再開", key=f"resume_{task['name']}"):
                                success, msg = resume_task_adhoc(task['name'])
                                if success:
                                    st.success(msg)
                                    st.rerun()
                                else:
                                    st.error(f"エラー: {msg}")
        else:
            st.info("登録済みのスケジュールはありません。")
    else:
        st.info("まず保存済み検索オブジェクトを作成してください。")

with tab4:
    st.subheader("⭐ お気に入り")
    
    # テーブルはsetup SQLで事前作成済み
    try:
        favorite_result = session.sql("SELECT * FROM application_db.application_schema.ADHOC_SEARCH_OBJECTS WHERE is_favorite = TRUE ORDER BY created_at DESC").collect()
        favorite_objects = [row.as_dict() for row in favorite_result]
    except Exception as e:
        st.error(f"お気に入りデータ取得エラー: {str(e)}")
        favorite_objects = []
        
    if favorite_objects:
        st.success(f"⭐ お気に入り: {len(favorite_objects)}件")
        
        for i, obj in enumerate(favorite_objects):
            # お気に入りアイコンの表示
            expander_title = f"⭐ {obj['OBJECT_NAME']} ({obj['TABLE1_NAME']} ⇄ {obj['TABLE2_NAME']})"
            
            with st.expander(expander_title, expanded=False):
                col1, col2 = st.columns([2, 3])
                with col1:
                    st.write(f"**説明**: {obj['DESCRIPTION'] or '説明なし'}")
                    st.write(f"**結合構成**: {obj['TABLE1_NAME']} {obj['JOIN_TYPE']} {obj['TABLE2_NAME']}")
                    st.write(f"**結合キー**: {obj['JOIN_KEY1']} = {obj['JOIN_KEY2']}")
                    st.write(f"**作成日**: {obj['CREATED_AT']}")
                    st.write(f"**実行回数**: {obj['EXECUTION_COUNT']}")
                    if obj['LAST_EXECUTED']:
                        st.write(f"**最終実行**: {obj['LAST_EXECUTED']}")
                    
                    # 実行機能
                    col_unfav, col_exec = st.columns(2)
                    
                    with col_unfav:
                        # お気に入り解除ボタン
                        if st.button("⭐ 解除", key=f"unfav_adhoc_{i}", type="secondary"):
                            toggle_adhoc_favorite(obj['OBJECT_ID'])
                            st.success("お気に入りを解除しました！")
                            st.rerun()
                    
                    with col_exec:
                        exec_limit = st.number_input("件数", min_value=1, max_value=500, value=50, step=50, key=f"fav_exec_limit_{i}")
                        if st.button("▶️ 実行", key=f"fav_exec_adhoc_{i}", type="primary", help="この検索を直接実行"):
                            # 実行回数を更新
                            update_adhoc_execution_count(obj['OBJECT_ID'])
                            
                            # クエリを実行
                            execute_query(obj['SEARCH_QUERY'], exec_limit)
                            
                            # タブ切り替えのフラグを設定
                            st.session_state.active_tab = "📄 検索結果"
                            st.session_state.show_result_from_saved = True
                            
                            st.success(f"✅ 「{obj['OBJECT_NAME']}」を実行しました！")
                            st.info("📄 結果は「検索結果」タブに表示されています。")
                            
                            # JavaScriptでタブ切り替えを試行
                            st.components.v1.html("""
                            <script>
                            setTimeout(function() {
                                // 検索結果タブ（最初のタブ）をクリック
                                const tabs = document.querySelectorAll('[data-testid="stTabs"] button');
                                if (tabs.length > 0) {
                                    tabs[0].click();
                                }
                            }, 100);
                            </script>
                            """, height=0)
                            
                            st.rerun()
                
                with col2:
                    st.markdown("**💻 保存されたSQL**")
                    st.code(obj['SEARCH_QUERY'], language="sql")
                    
                    # 結合の詳細情報
                    st.markdown("**🔗 結合詳細**")
                    st.info(f"**{obj['JOIN_TYPE']}**: `{obj['TABLE1_NAME']}.{obj['JOIN_KEY1']}` = `{obj['TABLE2_NAME']}.{obj['JOIN_KEY2']}`")
    else:
        st.info("⭐ お気に入りの非定型検索オブジェクトがありません。")
        st.info("保存済み検索から⭐ボタンをクリックしてお気に入りに追加してください。")

st.markdown("---")
st.markdown("**📊 Streamlitデータアプリ | 非定型検索 - ©Snowflake合同会社**")
