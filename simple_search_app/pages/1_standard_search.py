# =========================================================
# Snowflakeデータ操作アプリケーション
# 定型検索ページ
# =========================================================
# Created by kdaigo
# 最終更新: 2025/09/24
# 修正: スキーマを明示的に指定してテーブル一覧を取得
# =========================================================

import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit
import uuid

# ページ設定
st.set_page_config(
    layout="wide",
    page_title="🔍 定型検索",
    page_icon="🔍"
)

# Snowflakeセッション取得
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

# =========================================================
# セッション状態の初期化
# =========================================================
if 'new_selected_columns_state' not in st.session_state:
    st.session_state.new_selected_columns_state = set()
if 'last_result_df' not in st.session_state:
    st.session_state.last_result_df = None
if 'where_conditions_list' not in st.session_state:
    st.session_state.where_conditions_list = []
if 'order_by_conditions_list' not in st.session_state:
    st.session_state.order_by_conditions_list = []
if 'favorites' not in st.session_state:
    st.session_state.favorites = []
if 'execute_query_request' not in st.session_state:
    st.session_state.execute_query_request = None
if 'date_condition' not in st.session_state:
    st.session_state.date_condition = {}

# DB/スキーマ選択のセッション状態
if 'selected_database' not in st.session_state:
    st.session_state.selected_database = ""
if 'selected_schema' not in st.session_state:
    st.session_state.selected_schema = ""

# =========================================================
# ユーティリティ関数
# =========================================================
# テーブル作成関数は削除 - setup SQLで事前作成済み

def load_standard_search_objects():
    try:
        result = session.sql("SELECT * FROM application_db.application_schema.STANDARD_SEARCH_OBJECTS ORDER BY created_at DESC").collect()
        return [row.as_dict() for row in result]
    except:
        return []

def save_standard_search_object(object_data: dict):
    """定型検索オブジェクトを保存"""
    try:
        session.sql("""
        INSERT INTO application_db.application_schema.STANDARD_SEARCH_OBJECTS (
            object_id, object_name, description, search_query
        ) VALUES (?, ?, ?, ?)
        """, params=[
            object_data['object_id'],
            object_data['object_name'],
            object_data['description'],
            object_data['search_query']
        ]).collect()
        return True
    except Exception as e:
        st.error(f"保存エラー: {str(e)}")
        return False

def execute_standard_search(object_id: str):
    try:
        result = session.sql("SELECT * FROM application_db.application_schema.STANDARD_SEARCH_OBJECTS WHERE object_id = ?", params=[object_id]).collect()
        if not result:
            return False, "検索オブジェクトが見つかりません"
        search_obj = result[0].as_dict()
        search_query = search_obj['SEARCH_QUERY']
        search_result = session.sql(search_query).collect()
        session.sql("""
        UPDATE application_db.application_schema.STANDARD_SEARCH_OBJECTS 
        SET execution_count = execution_count + 1, 
            last_executed = CURRENT_TIMESTAMP()
        WHERE object_id = ?
        """, params=[object_id]).collect()
        return True, search_result
    except Exception as e:
        return False, str(e)

def update_execution_count(object_id: str):
    """実行回数を更新する専用関数"""
    try:
        session.sql("""
        UPDATE application_db.application_schema.STANDARD_SEARCH_OBJECTS 
        SET execution_count = execution_count + 1, 
            last_executed = CURRENT_TIMESTAMP()
        WHERE object_id = ?
        """, params=[object_id]).collect()
        return True
    except Exception as e:
        st.error(f"実行回数更新エラー: {str(e)}")
        return False


def add_to_favorites(object_id: str):
    try:
        session.sql("""
        UPDATE application_db.application_schema.STANDARD_SEARCH_OBJECTS 
        SET is_favorite = TRUE 
        WHERE object_id = ?
        """, params=[object_id]).collect()
        return True
    except:
        return False

# =========================================================
# ユーティリティ関数（キャッシュ対応）
# =========================================================
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

@st.cache_data(ttl=300, show_spinner=False)
def get_table_columns_with_types_cached(table_name: str):
    """テーブル/ビューのカラム名とデータ型を取得（5分キャッシュ）"""
    try:
        # 日本語テーブル名に対応するためダブルクォーテーションで囲む
        quoted_table_name = f'"{table_name}"' if not table_name.startswith('"') else table_name
        # テーブルのスキーマを動的に判定
        schema = get_table_schema(table_name)
        result = session.sql(f"DESCRIBE TABLE {schema}.{quoted_table_name}").collect()
        return [{'name': row['name'], 'type': row['type']} for row in result]
    except Exception as e:
        st.error(f"テーブル情報取得エラー ({table_name}): {str(e)}")
        return []


def parse_relation_label(label: str) -> str:
    """[TABLE]/[VIEW] ラベルからオブジェクト名のみ取り出す"""
    return label.split(' ', 1)[1] if ' ' in label else label

def quote_identifier(identifier: str) -> str:
    """SQL識別子（テーブル名、カラム名）を適切にクォートする"""
    if not identifier:
        return identifier
    
    # 前後の空白、改行、特殊文字をトリム
    identifier = identifier.strip().strip('\n\r\t')
    
    # 既にクォートされている場合はそのまま返す
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier
    
    # 安全のため、すべての識別子をダブルクォートで囲む
    # （CSVインジェストしたデータなど、様々なソースからのカラム名に対応）
    # 内部のダブルクォートをエスケープ
    escaped_identifier = identifier.replace('"', '""')
    return f'"{escaped_identifier}"'

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

def get_column_data_type(table_cols: list, column_name: str) -> str:
    """指定されたカラムのデータ型を取得する"""
    for col in table_cols:
        if col['name'] == column_name:
            return col['type']
    return ""


# =========================================================
# 実行ロジック
# =========================================================
def execute_query(search_query: str, all_rows: bool, limit_rows: int, show_sql: bool):
    """
    クエリを実行し、結果をセッション状態に保存する
    """
    def _sanitize_query(q: str) -> str:
        return q.strip().rstrip(';')
    
    def _fix_japanese_identifiers(query: str) -> str:
        """日本語のテーブル名・カラム名を自動的にクォートする（改良版）"""
        import re
        
        # 既にquote_identifierで生成されたクエリかチェック
        if '""' in query:
            # 二重クォートを修正
            query = query.replace('""', '"')
        
        # 日本語文字を含む識別子パターン（ただし既にクォートされていないもの）
        japanese_pattern = r'[あ-んア-ンー一-龯]+'
        
        # FROM句のテーブル名をクォート（既にクォートされていない場合のみ）
        def quote_table_name(match):
            full_match = match.group(0)
            table_name = match.group(1)
            if re.search(japanese_pattern, table_name) and not table_name.startswith('"'):
                return f'FROM "{table_name}"'
            return full_match
        
        query = re.sub(r'FROM\s+([^\s\'"]+)', quote_table_name, query, flags=re.IGNORECASE)
        
        # WHERE句のカラム名をクォート（値の部分は除外）
        def quote_where_column(match):
            full_match = match.group(0)
            col_name = match.group(1)
            if re.search(japanese_pattern, col_name) and not col_name.startswith('"'):
                return full_match.replace(col_name, f'"{col_name}"')
            return full_match
        
        # WHERE, AND, OR句での日本語カラム名をクォート（等号の前の部分のみ）
        query = re.sub(r'(WHERE|AND|OR)\s+([^\s\'"=<>!]+)\s*([=<>!]+)', quote_where_column, query, flags=re.IGNORECASE)
        
        return query

    try:
        base_query = _sanitize_query(search_query)
        
        # 保存時にquote_identifierで既に正しく処理されているため、
        # 実行時の自動修正は行わない（二重処理を避ける）
        final_query = base_query
        
        if (not all_rows) and " LIMIT " not in base_query.upper():
            final_query = f"{base_query} LIMIT {int(limit_rows)}"
        
        # SQL表示（show_sqlがTrueの場合）
        if show_sql:
            st.markdown("### 📝 実行SQL")
            st.code(final_query, language="sql")
            
            # 詳細情報も表示
            if base_query != final_query:
                with st.expander("🔍 SQL詳細情報", expanded=False):
                    st.write("**元のクエリ:**")
                    st.code(base_query, language="sql")
                    st.write("**LIMIT句追加後:**")
                    st.code(final_query, language="sql")

        with st.spinner("検索実行中..."):
            # まず件数チェック
            try:
                test_query = f"SELECT COUNT(*) FROM ({final_query})"
                row_count = session.sql(test_query).collect()[0][0]
                
                if row_count > 5000:
                    st.warning(f"検索結果が5,000行を超えています。表示に時間がかかる場合があります。取得件数: {row_count} 行")
                elif row_count == 0:
                    st.warning("検索条件に該当するデータがありません。")
                    
            except Exception as count_error:
                st.error(f"件数チェックエラー: {str(count_error)}")
                st.write("件数チェック用SQL:")
                st.code(test_query, language="sql")
                return

            # データ取得実行
            try:
                df_result = session.sql(final_query).to_pandas()
                st.session_state.last_result_df = df_result
                st.success(f"✅ 取得件数: {len(df_result)} 行。下部の『📄 出力結果』に表示しました。")
            except Exception as data_error:
                st.error(f"データ取得エラー: {str(data_error)}")
                st.write("データ取得用SQL:")
                st.code(final_query, language="sql")
                return

    except Exception as e:
        st.error(f"検索エラー: {str(e)}")
        st.write("実行クエリの参考:")
        try:
            st.code(final_query, language="sql")
        except:
            st.code(base_query, language="sql")

# =========================================================
# アプリケーション本体
# =========================================================

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
        key="std_search_db_select"
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
            key="std_search_schema_select"
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

# タイトル
st.title("🔍 定型検索")
st.header("事前定義された検索テンプレートの管理と実行")

# ---
# 新規作成（メイン画面ワイドUI）
# ---
st.markdown("---")
st.subheader("➕ 新規検索オブジェクト作成")

colL, colR = st.columns([2, 3])
with colL:
    new_object_name = st.text_input("オブジェクト名", key="new_object_name", placeholder="例：口座を保有する東京都在住プレミア顧客の抽出")
    new_description = st.text_area("説明", key="new_description", placeholder="例：東京都在住のプレミアムランクの顧客データを抽出します。")
    
    relations = get_available_relations()
    selected_relation_label = st.selectbox("テーブル/ビューを選択", relations, key="new_relation_select")
    selected_table = parse_relation_label(selected_relation_label) if selected_relation_label else ""

      # 日付指定ブロック（独立・必須）
    st.markdown("#### 📅 日付指定（必須）")
    if selected_table:
        table_cols = get_table_columns_with_types_cached(selected_table)
        
        # 日付型カラムを抽出（データ型とカラム名の両方でチェック）
        date_columns = [col for col in table_cols if is_date_like_column(col['name'], col['type'])]
        
        if date_columns:
            st.info(f"📅 日付型カラムが {len(date_columns)} 件見つかりました")
            
            # 日付カラム選択
            date_col_options = [""] + [f"{col['name']} ({col['type']})" for col in date_columns]
            selected_date_col_label = st.selectbox(
                "日付カラムを選択",
                date_col_options,
                key="date_col_select",
                help="検索対象の日付カラムを選択してください"
            )
            
            if selected_date_col_label:
                # カラム名を抽出
                selected_date_col = selected_date_col_label.split(" (")[0]
                
                # 日付範囲指定
                col_date1, col_date2 = st.columns(2)
                with col_date1:
                    start_date = st.date_input(
                        "開始日",
                        value=datetime.now().date() - timedelta(days=30),  # デフォルト30日前
                        key="date_start"
                    )
                with col_date2:
                    end_date = st.date_input(
                        "終了日",
                        value=datetime.now().date(),
                        key="date_end"
                    )
                
                # 日付範囲の検証
                if start_date and end_date:
                    if start_date > end_date:
                        st.error("❌ 開始日は終了日より前の日付を指定してください")
                    else:
                        st.success(f"📅 検索期間: {start_date} 〜 {end_date} ({end_date - start_date + timedelta(days=1)}日間)")
                        
                        # 日付条件をセッション状態に保存
                        if 'date_condition' not in st.session_state:
                            st.session_state.date_condition = {}
                        
                        st.session_state.date_condition = {
                            "column": selected_date_col,
                            "start_date": start_date.strftime('%Y-%m-%d'),
                            "end_date": end_date.strftime('%Y-%m-%d')
                        }
        else:
            st.warning("⚠️ このテーブルには日付型カラムが見つかりませんでした")
            st.info("日付型カラムがない場合は、通常のフィルター条件を使用してください")
    else:
        st.info("テーブル/ビューを選択すると日付指定が可能になります")
        
    # WHERE句のGUI入力部分（日付以外の条件）
    st.markdown("#### フィルター条件 (WHERE句)")
    if selected_table:
        table_cols = get_table_columns_with_types_cached(selected_table)
        
        # 既存の条件の表示
        for i, condition in enumerate(st.session_state.where_conditions_list):
            op = "WHERE" if i == 0 else condition['logic_op']
            quoted_col = quote_identifier(condition['column'])
            st.write(f"**{op.upper()}** `{quoted_col}` {condition['operator']} `'{condition['value']}'`")
            if st.button("🗑️", key=f"del_cond_{i}"):
                del st.session_state.where_conditions_list[i]
                st.rerun()

        # 新しい条件の追加フォーム（日付以外）
        with st.expander("➕ 新しい条件を追加"):
            cond_logic_op = st.selectbox("論理演算子", ["AND", "OR"], key="cond_logic_op", disabled=(len(st.session_state.where_conditions_list) == 0))
            
            # 日付型以外のカラムのみを表示
            non_date_columns = [col for col in table_cols if not is_date_like_column(col['name'], col['type'])]
            cond_col_name = st.selectbox("カラムを選択", [""] + sorted([c['name'] for c in non_date_columns]), key="cond_col_name")
            cond_operator = st.selectbox("演算子を選択", ["=", ">", "<", ">=", "<=", "<>", "LIKE"], key="cond_operator")
            cond_value = st.text_input("値を入力", key="cond_value")
            
            if st.button("追加", key="add_condition_btn") and cond_col_name and cond_value:
                st.session_state.where_conditions_list.append({
                    "logic_op": cond_logic_op,
                    "column": cond_col_name,
                    "operator": cond_operator,
                    "value": cond_value
                })
                st.success("条件を追加しました！")
                st.rerun()
                
                
    # ORDER BY句のGUI入力部分
    st.markdown("#### ソート条件 (ORDER BY句)")
    if selected_table:
        # 既存のソート条件の表示
        for i, condition in enumerate(st.session_state.order_by_conditions_list):
            quoted_col = quote_identifier(condition['column'])
            st.write(f"**ORDER BY** `{quoted_col}` **{condition['direction']}**")
            if st.button("🗑️", key=f"del_sort_{i}"):
                del st.session_state.order_by_conditions_list[i]
                st.rerun()

        # 新しいソート条件の追加フォーム
        with st.expander("➕ 新しいソート条件を追加"):
            sort_col_name = st.selectbox("ソート対象カラムを選択", [""] + sorted([c['name'] for c in table_cols]), key="sort_col_name")
            sort_direction = st.selectbox("ソート方向を選択", ["ASC", "DESC"], key="sort_direction", help="ASC: 昇順（小→大）、DESC: 降順（大→小）")
            
            if st.button("追加", key="add_sort_btn") and sort_col_name:
                st.session_state.order_by_conditions_list.append({
                    "column": sort_col_name,
                    "direction": sort_direction
                })
                st.success("ソート条件を追加しました！")
                st.rerun()
    else:
        st.info("テーブル/ビューを選択すると条件を設定できます。")

with colR:
    st.markdown("#### 出力項目 (SELECT句)")
    selected_columns = []
    if selected_table:
        basic_cols = get_table_columns_with_types_cached(selected_table)
        cols_with_info = [{'name': c['name'], 'type': c['type']} for c in basic_cols]
        
        filter_text = st.text_input("カラム検索（部分一致）", key="col_filter_main")
        if filter_text:
            cols_with_info = [c for c in cols_with_info if filter_text.lower() in c['name'].lower()]
        
        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅ 全選択", key="btn_select_all_cols_main"):
                st.session_state.new_selected_columns_state = set([c['name'] for c in cols_with_info])
                st.rerun()
        with c2:
            if st.button("🧹 全解除", key="btn_clear_cols_main"):
                st.session_state.new_selected_columns_state = set()
                st.rerun()

        display_data = []
        for c in cols_with_info:
            is_selected = c['name'] in st.session_state.new_selected_columns_state
            display_row = {
                '選択': is_selected,
                'カラム名': c['name'],
                'データ型': c['type']
            }
            display_data.append(display_row)
        
        df_cols = pd.DataFrame(display_data)

        if not df_cols.empty:
            column_config = {
                "選択": st.column_config.CheckboxColumn(
                    "選択",
                    help="表示するカラムを選択",
                    default=False
                ),
                "カラム名": st.column_config.TextColumn("カラム名", width="medium"),
                "データ型": st.column_config.TextColumn("データ型", width="small")
            }

            edited_df = st.data_editor(
                df_cols,
                column_config=column_config,
                hide_index=True,
                use_container_width=True,
                key="column_selection_editor"
            )

            selected_names = {row['カラム名'] for _, row in edited_df.iterrows() if row['選択']}
            st.session_state.new_selected_columns_state = selected_names
            
            selected_columns = sorted(list(st.session_state.new_selected_columns_state))
    else:
        st.info("テーブル/ビューを選択すると、カラム一覧が表示されます。")


# SQLプレビュー・保存
st.markdown("---")
colA, colB = st.columns([1, 2])
with colA:
    # 保存条件の判定
    has_date_condition = 'date_condition' in st.session_state and st.session_state.date_condition
    can_save = new_object_name and selected_table and has_date_condition
    
    if st.button("💾 保存", key="save_new_object_main", disabled=not can_save):
        # WHERE句の生成
        where_clauses = []
        
        # 日付条件を最初に追加（必須）
        if 'date_condition' in st.session_state and st.session_state.date_condition:
            date_cond = st.session_state.date_condition
            quoted_date_col = quote_identifier(date_cond['column'])
            date_clause = f"{quoted_date_col} BETWEEN '{date_cond['start_date']}' AND '{date_cond['end_date']}'"
            where_clauses.append(date_clause)
        
        # その他の条件を追加
        for i, cond in enumerate(st.session_state.where_conditions_list):
            quoted_col = quote_identifier(cond['column'])
            cond_str = f"{quoted_col} {cond['operator']}"
            if cond['operator'].upper() == 'LIKE':
                cond_str += f" '%{cond['value']}%'"
            else:
                cond_str += f" '{cond['value']}'"
            
            # 最初の条件以外は論理演算子を追加
            if where_clauses:  # 日付条件がある場合はANDを追加
                where_clauses.append(f"AND {cond_str}")
            else:
                where_clauses.append(cond_str)
        
        where_clause = " WHERE " + " ".join(where_clauses) if where_clauses else ""
        
        # ORDER BY句の生成
        order_by_clauses = []
        for cond in st.session_state.order_by_conditions_list:
            quoted_col = quote_identifier(cond['column'])
            order_by_clauses.append(f"{quoted_col} {cond['direction']}")
        
        order_by_clause = " ORDER BY " + ", ".join(order_by_clauses) if order_by_clauses else ""
        
        # SELECT句でカラム名をクォート
        if selected_columns:
            quoted_columns = [quote_identifier(col) for col in selected_columns]
            select_clause = ", ".join(quoted_columns)
        else:
            select_clause = "*"
        
        # テーブル名もクォート（スキーマを含む完全修飾名を使用）
        quoted_table = quote_identifier(selected_table)
        table_schema = get_table_schema(selected_table)
        generated_query = f"SELECT {select_clause} FROM {table_schema}.{quoted_table}{where_clause}{order_by_clause}"

        object_data = {
            'object_id': f"obj_{uuid.uuid4().hex[:12]}",
            'object_name': new_object_name,
            'description': new_description,
            'search_query': generated_query
        }
        if save_standard_search_object(object_data):
            st.success("検索オブジェクトを保存しました！")
            st.session_state.new_selected_columns_state = set()
            st.session_state.where_conditions_list = []
            st.session_state.order_by_conditions_list = []
            st.session_state.date_condition = {}
            st.rerun()
    if not can_save:
        if not new_object_name:
            st.warning("オブジェクト名を入力してください。")
        elif not selected_table:
            st.warning("テーブル/ビューを選択してください。")
        elif not has_date_condition:
            st.warning("📅 日付指定（必須）を設定してください。")

with colB:
    st.markdown("#### 📝 SQLプレビュー")
    if selected_table:
        # WHERE句の生成
        where_clauses = []
        
        # 日付条件を最初に追加（必須）
        if 'date_condition' in st.session_state and st.session_state.date_condition:
            date_cond = st.session_state.date_condition
            quoted_date_col = quote_identifier(date_cond['column'])
            date_clause = f"{quoted_date_col} BETWEEN '{date_cond['start_date']}' AND '{date_cond['end_date']}'"
            where_clauses.append(date_clause)
        
        # その他の条件を追加
        for i, cond in enumerate(st.session_state.where_conditions_list):
            quoted_col = quote_identifier(cond['column'])
            cond_str = f"{quoted_col} {cond['operator']}"
            if cond['operator'].upper() == 'LIKE':
                cond_str += f" '%{cond['value']}%'"
            else:
                cond_str += f" '{cond['value']}'"
            
            # 最初の条件以外は論理演算子を追加
            if where_clauses:  # 日付条件がある場合はANDを追加
                where_clauses.append(f"AND {cond_str}")
            else:
                where_clauses.append(cond_str)
        
        where_clause = " WHERE " + " ".join(where_clauses) if where_clauses else ""
        
        # ORDER BY句の生成
        order_by_clauses = []
        for cond in st.session_state.order_by_conditions_list:
            quoted_col = quote_identifier(cond['column'])
            order_by_clauses.append(f"{quoted_col} {cond['direction']}")
        
        order_by_clause = " ORDER BY " + ", ".join(order_by_clauses) if order_by_clauses else ""
        
        # SELECT句でカラム名をクォート
        if selected_columns:
            quoted_columns = [quote_identifier(col) for col in selected_columns]
            select_clause = ", ".join(quoted_columns)
        else:
            select_clause = "*"
        
        # テーブル名もクォート（スキーマを含む完全修飾名を使用）
        quoted_table = quote_identifier(selected_table)
        table_schema = get_table_schema(selected_table)
        generated_query = f"SELECT {select_clause} FROM {table_schema}.{quoted_table}{where_clause}{order_by_clause}"
        st.code(generated_query, language="sql")
        
        # ソート条件がある場合は追加情報を表示
        if order_by_clauses:
            st.info(f"📊 ソート条件: {len(order_by_clauses)}件設定済み")
    else:
        st.info("テーブル/ビューを選択するとSQLプレビューが表示されます。")

st.markdown("---")

# =========================================================
# タブ
# =========================================================
tab1, tab3 = st.tabs(["📋 オブジェクト一覧", "⭐ お気に入り"])
# tab2 = スケジュール実行タブ（機能不要のためコメントアウト）

with tab1:
    st.subheader("📋 定型検索オブジェクト一覧")
    # テーブルはsetup SQLで事前作成済み
    objects = load_standard_search_objects()
    if objects:
        for i, obj in enumerate(objects):
            with st.expander(f"🔍 {obj['OBJECT_NAME']} ({obj['OBJECT_ID']})", expanded=False):
                col1, col2 = st.columns([3, 2])
                with col1:
                    st.write(f"**説明**: {obj['DESCRIPTION'] or '説明なし'}")
                    # 作成日を日時（hh:mm）まで表示
                    created_at = obj['CREATED_AT']
                    if created_at:
                        if isinstance(created_at, str):
                            try:
                                from datetime import datetime
                                created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                                formatted_date = created_dt.strftime('%Y-%m-%d %H:%M')
                            except:
                                formatted_date = str(created_at)[:16]  # フォールバック
                        else:
                            formatted_date = created_at.strftime('%Y-%m-%d %H:%M')
                    else:
                        formatted_date = "不明"
                    st.write(f"**作成日**: {formatted_date}")
                    st.write(f"**実行回数**: {obj['EXECUTION_COUNT']}")
                    if obj['LAST_EXECUTED']:
                        last_executed = obj['LAST_EXECUTED']
                        if isinstance(last_executed, str):
                            try:
                                last_dt = datetime.fromisoformat(last_executed.replace('Z', '+00:00'))
                                formatted_last = last_dt.strftime('%Y-%m-%d %H:%M')
                            except:
                                formatted_last = str(last_executed)[:16]
                        else:
                            formatted_last = last_executed.strftime('%Y-%m-%d %H:%M')
                        st.write(f"**最終実行**: {formatted_last}")
                    
                    with col2:
                        all_rows = st.checkbox("全件取得 (LIMIT無効、非推奨)", value=False, key=f"allrows_{i}")
                        limit_rows = st.number_input("LIMIT行数", min_value=10, max_value=1000, value=100, step=50, key=f"limit_{i}", disabled=all_rows)
                        show_sql = st.checkbox("SQLを表示", value=False, key=f"show_sql_{i}")
                        
                        # SQLを表示（チェックボックスがONの場合、即座に表示）
                        if show_sql:
                            st.markdown("**📝 実行予定SQL:**")
                            # LIMIT句を考慮したSQLを生成
                            base_query = obj['SEARCH_QUERY']
                            if not all_rows and " LIMIT " not in base_query.upper():
                                display_query = f"{base_query} LIMIT {int(limit_rows)}"
                            else:
                                display_query = base_query
                            st.code(display_query, language="sql")
                        
                        # ボタンがクリックされたときの処理
                        if st.button("▶️ 実行", key=f"exec_btn_{i}"):
                            # 実行回数を更新
                            update_execution_count(obj['OBJECT_ID'])
                            
                            # 実行に必要な情報をセッション状態に保存し、再実行を要求
                            st.session_state.execute_query_request = {
                                "query": obj['SEARCH_QUERY'],
                                "all_rows": all_rows,
                                "limit_rows": limit_rows,
                                "show_sql": show_sql,
                            }
                            st.rerun()

                    fav_col = st.columns(1)[0]
                    with fav_col:
                        if 'favorites' not in st.session_state:
                            st.session_state.favorites = []
                        if obj['IS_FAVORITE']:
                            st.write("⭐ お気に入り済み")
                        else:
                            if st.button("⭐ お気に入り", key=f"favorite_{obj['OBJECT_ID']}_{i}"):
                                if add_to_favorites(obj['OBJECT_ID']):
                                    st.success("お気に入りに追加しました！")
                                    st.rerun()
    else:
        st.info("定型検索オブジェクトがありません。新規作成してください。")


with tab3:
    st.subheader("⭐ お気に入り")
    # テーブルはsetup SQLで事前作成済み
    favorite_objects = session.sql("SELECT * FROM application_db.application_schema.STANDARD_SEARCH_OBJECTS WHERE is_favorite = TRUE ORDER BY created_at DESC").collect()
    if favorite_objects:
            st.success(f"お気に入り: {len(favorite_objects)}件")
            for i, obj in enumerate(favorite_objects):
                with st.expander(f"⭐ {obj['OBJECT_NAME']} ({obj['OBJECT_ID']})", expanded=False):
                    col1, col2 = st.columns([3, 2])
                    with col1:
                        st.write(f"**説明**: {obj['DESCRIPTION'] or '説明なし'}")
                        # 作成日を日時（hh:mm）まで表示
                        created_at = obj['CREATED_AT']
                        if created_at:
                            if isinstance(created_at, str):
                                try:
                                    from datetime import datetime
                                    created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                                    formatted_date = created_dt.strftime('%Y-%m-%d %H:%M')
                                except:
                                    formatted_date = str(created_at)[:16]  # フォールバック
                            else:
                                formatted_date = created_at.strftime('%Y-%m-%d %H:%M')
                        else:
                            formatted_date = "不明"
                        st.write(f"**作成日**: {formatted_date}")
                        st.write(f"**実行回数**: {obj['EXECUTION_COUNT']}")
                        if obj['LAST_EXECUTED']:
                            last_executed = obj['LAST_EXECUTED']
                            if isinstance(last_executed, str):
                                try:
                                    last_dt = datetime.fromisoformat(last_executed.replace('Z', '+00:00'))
                                    formatted_last = last_dt.strftime('%Y-%m-%d %H:%M')
                                except:
                                    formatted_last = str(last_executed)[:16]
                            else:
                                formatted_last = last_executed.strftime('%Y-%m-%d %H:%M')
                            st.write(f"**最終実行**: {formatted_last}")
                    with col2:
                        all_rows = st.checkbox("全件取得 (LIMIT無効、非推奨)", value=False, key=f"fav_allrows_{i}")
                        limit_rows = st.number_input("LIMIT行数", min_value=10, max_value=5000, value=5000, step=10, key=f"fav_limit_{i}", disabled=all_rows)
                        show_sql = st.checkbox("SQLを表示", value=False, key=f"fav_show_sql_{i}")

                        # SQLを表示（チェックボックスがONの場合、即座に表示）
                        if show_sql:
                            st.markdown("**📝 実行予定SQL:**")
                            # LIMIT句を考慮したSQLを生成
                            base_query = obj['SEARCH_QUERY']
                            if not all_rows and " LIMIT " not in base_query.upper():
                                display_query = f"{base_query} LIMIT {int(limit_rows)}"
                            else:
                                display_query = base_query
                            st.code(display_query, language="sql")

                        if st.button("▶️ 実行", key=f"fav_exec_btn_{i}"):
                            # 実行回数を更新
                            update_execution_count(obj['OBJECT_ID'])
                            
                            st.session_state.execute_query_request = {
                                "query": obj['SEARCH_QUERY'],
                                "all_rows": all_rows,
                                "limit_rows": limit_rows,
                                "show_sql": show_sql,
                            }
                            st.rerun()
    else:
        st.info("お気に入りの検索オブジェクトがありません。")
        st.info("検索オブジェクト一覧から⭐ボタンをクリックしてお気に入りに追加してください。")

# =========================================================
# セッション状態のクエリ実行リクエストを処理
# =========================================================
if st.session_state.execute_query_request is not None:
    request = st.session_state.execute_query_request
    execute_query(
        search_query=request["query"],
        all_rows=request["all_rows"],
        limit_rows=request["limit_rows"],
        show_sql=request["show_sql"]
    )
    # リクエストを初期化してループを防ぐ
    st.session_state.execute_query_request = None

# =========================================================
# 大きな帳票形式の出力結果ビューア
# =========================================================
st.markdown("---")
st.subheader("📄 出力結果")
if st.session_state.last_result_df is not None:
    st.dataframe(st.session_state.last_result_df, use_container_width=True, height=600)
    csv = st.session_state.last_result_df.to_csv(index=False)
    st.download_button(label="💾 CSVダウンロード", data=csv, file_name=f"result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv")
else:
    st.info("ここに最新の実行結果を表示します。上部で検索を実行してください。")

st.markdown("---")
st.markdown("**📊 Streamlitデータアプリ | 定型検索 - ©Snowflake合同会社**")
