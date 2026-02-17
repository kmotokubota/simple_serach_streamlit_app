# =========================================================
# Snowflakeデータ操作アプリケーション
# データ取込ページ
# =========================================================
# Created by kdaigo
# 最終更新: 2025/09/24
# =========================================================
import streamlit as st
import pandas as pd
import time
import io
from datetime import datetime
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import col

st.set_page_config(layout="wide", page_title="📥 データ取込", page_icon="📥")

@st.cache_resource
def get_snowflake_session():
    return get_active_session()

session = get_snowflake_session()

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

def get_current_data_schema():
    """現在選択されているデータスキーマを取得（DB.SCHEMA形式）"""
    if st.session_state.get('selected_database') and st.session_state.get('selected_schema'):
        return f"{st.session_state.selected_database}.{st.session_state.selected_schema}"
    return "application_db.application_schema"  # デフォルト

# セッション状態の初期化
if 'uploaded_files' not in st.session_state:
    st.session_state.uploaded_files = []
if 'import_scripts' not in st.session_state:
    st.session_state.import_scripts = []
if 'current_df' not in st.session_state:
    st.session_state.current_df = None
if 'inferred_schema' not in st.session_state:
    st.session_state.inferred_schema = []
if 'table_name' not in st.session_state:
    st.session_state.table_name = ""

# DB/スキーマ選択のセッション状態
if 'selected_database' not in st.session_state:
    st.session_state.selected_database = ""
if 'selected_schema' not in st.session_state:
    st.session_state.selected_schema = ""

def check_table_exists(table_name: str) -> bool:
    """テーブルの存在確認（選択されたスキーマ内で確認）"""
    try:
        current_schema = get_current_data_schema()
        result = session.sql(f"SHOW TABLES LIKE '{table_name}' IN {current_schema}").collect()
        return len(result) > 0
    except:
        return False

def infer_schema(df: pd.DataFrame) -> list:
    """CSVデータからスキーマを推測する"""
    schema = []
    for col_name in df.columns:
        col_data = df[col_name]
        
        # データ型を推測
        if col_data.dtype == 'object':
            # 文字列型の場合、フル桁のVARCHARを使用
            data_type = "VARCHAR(16777216)"  # Snowflakeの最大VARCHAR長
        elif col_data.dtype in ['int64', 'int32']:
            data_type = "NUMBER"
        elif col_data.dtype in ['float64', 'float32']:
            data_type = "FLOAT"
        elif col_data.dtype == 'bool':
            data_type = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(col_data):
            data_type = "DATE"
        else:
            data_type = "VARCHAR(16777216)"
        
        schema.append({
            'column_name': col_name,
            'data_type': data_type,
            'sample_data': str(col_data.iloc[0]) if len(col_data) > 0 else ""
        })
    
    return schema

def create_table_sql(table_name: str, schema: list) -> str:
    """CREATE TABLE文を生成する"""
    columns = []
    for col in schema:
        # カラム名をダブルクォートで囲み、データ型はそのまま使用
        column_def = f'"{col["column_name"]}" {col["data_type"]}'
        columns.append(column_def)
    
    # 選択されたスキーマに作成
    current_schema = get_current_data_schema()
    full_table_name = f"{current_schema}.{table_name}"
    sql = f"CREATE OR REPLACE TABLE {full_table_name} (\n"
    sql += ",\n".join([f"    {col}" for col in columns])
    sql += "\n)"
    
    return sql

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
        key="ingest_db_select"
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
            key="ingest_schema_select"
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
    st.sidebar.success(f"📁 保存先: {st.session_state.selected_database}.{st.session_state.selected_schema}")

st.sidebar.markdown("---")

st.title("📥 データ取込")
st.header("CSV、Excel、固定長ファイルの取込とスクリプト管理")

# メイン機能
st.subheader("📁 CSVファイルアップロード")
st.info("CSVファイルをアップロードして、スキーマを推測・編集後にSnowflakeテーブルとして保存します")

# Step 1: ファイルアップロード
uploaded_file = st.file_uploader("CSVファイルを選択してください", type=['csv'])

if uploaded_file is not None:
    try:
        # CSVファイルを読み込み
        df = pd.read_csv(uploaded_file)
        st.session_state.current_df = df
        
        st.success(f"✅ ファイル読み込み完了: {uploaded_file.name}")
        st.write(f"**行数**: {len(df):,} 行, **列数**: {len(df.columns)} 列")
        
        # データプレビュー
        with st.expander("📊 データプレビュー", expanded=True):
            st.dataframe(df.head(10), use_container_width=True)
        
        # Step 2: テーブル名入力
        st.subheader("🏷️ テーブル名の設定")
        default_table_name = f"IMPORT_{uploaded_file.name.split('.')[0].upper().replace('-', '_').replace(' ', '_')}"
        table_name = st.text_input(
            "テーブル名を入力してください", 
            value=default_table_name,
            help="英数字とアンダースコアのみ使用可能です"
        )
        st.session_state.table_name = table_name
        
        # テーブル名の検証
        if table_name:
            if check_table_exists(table_name):
                st.warning(f"⚠️ テーブル '{table_name}' は選択中のスキーマに既に存在します。保存時に上書きされます。")
        
        # Step 3: スキーマ推測と編集
        if table_name:
            st.subheader("🔍 スキーマ編集")
            
            # スキーマ推測ボタン
            if st.button("🔄 スキーマを推測", type="secondary"):
                st.session_state.inferred_schema = infer_schema(df)
                st.success("スキーマを推測しました！")
            
            # スキーマが推測されている場合、編集可能な表示
            if st.session_state.inferred_schema:
                st.write("**推測されたスキーマ（編集可能）:**")
                
                # データ型の選択肢
                data_type_options = [
                    "VARCHAR(16777216)",
                    "NUMBER", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP"
                ]
                
                # スキーマ編集用のコンテナ
                schema_container = st.container()
                
                with schema_container:
                    # ヘッダー行
                    col1, col2, col3, col4 = st.columns([3, 3, 2, 2])
                    with col1:
                        st.write("**列名**")
                    with col2:
                        st.write("**データ型**")
                    with col3:
                        st.write("**サンプルデータ**")
                    with col4:
                        st.write("**エラー**")
                    
                    # 各列のスキーマを編集可能にする
                    updated_schema = []
                    for i, schema_item in enumerate(st.session_state.inferred_schema):
                        col1, col2, col3, col4 = st.columns([3, 3, 2, 2])
                        
                        with col1:
                            column_name = st.text_input(
                                f"列名_{i}", 
                                value=schema_item['column_name'],
                                key=f"col_name_{i}",
                                label_visibility="collapsed"
                            )
                        
                        with col2:
                            # データ型の選択肢で現在の値が存在するかチェック
                            current_data_type = schema_item['data_type']
                            if current_data_type not in data_type_options:
                                # 古い形式のVARCHARを新しい形式に変換
                                if current_data_type.startswith("VARCHAR"):
                                    current_data_type = "VARCHAR(16777216)"
                                else:
                                    current_data_type = data_type_options[0]  # デフォルトを設定
                            
                            data_type = st.selectbox(
                                f"データ型_{i}",
                                options=data_type_options,
                                index=data_type_options.index(current_data_type),
                                key=f"data_type_{i}",
                                label_visibility="collapsed"
                            )
                        
                        with col3:
                            st.text(schema_item['sample_data'][:20] + "..." if len(schema_item['sample_data']) > 20 else schema_item['sample_data'])
                        
                        with col4:
                            # エラーチェック（文字列データが含まれる場合の警告など）
                            error_msg = ""
                            if data_type.startswith("NUMBER") or data_type == "FLOAT":
                                try:
                                    pd.to_numeric(df[schema_item['column_name']], errors='raise')
                                except:
                                    error_msg = "⚠️"
                            st.text(error_msg)
                        
                        updated_schema.append({
                            'column_name': column_name,
                            'data_type': data_type,
                            'sample_data': schema_item['sample_data']
                        })
                    
                    # 更新されたスキーマを保存
                    st.session_state.inferred_schema = updated_schema
                
                # Step 4: SQL プレビュー
                st.subheader("📝 生成されるSQL")
                create_sql = create_table_sql(table_name, st.session_state.inferred_schema)
                st.code(create_sql, language="sql")
                
                # Step 5: 保存実行
                st.subheader("💾 テーブル保存")
                col1, col2 = st.columns([1, 1])
                
                with col1:
                    if st.button("🚀 テーブルを保存", type="primary", use_container_width=True):
                        try:
                            with st.spinner("テーブルを作成中..."):
                                # テーブル作成
                                session.sql(create_sql).collect()
                                
                                # データ挿入用のSnowparkデータフレーム作成
                                snowpark_df = session.create_dataframe(df)
                                
                                # データをテーブルに挿入（選択されたスキーマに保存）
                                current_schema = get_current_data_schema()
                                full_table_name = f"{current_schema}.{table_name}"
                                snowpark_df.write.mode("overwrite").save_as_table(full_table_name)
                                
                            st.success(f"✅ テーブル '{table_name}' が正常に作成されました！")
                            st.balloons()
                            
                            # 結果確認
                            result_df = session.table(full_table_name).limit(5).to_pandas()
                            st.write("**保存されたデータ（先頭5行）:**")
                            st.dataframe(result_df, use_container_width=True)
                            
                        except Exception as e:
                            st.error(f"❌ テーブル保存エラー: {str(e)}")
                
                with col2:
                    if st.button("🗑️ リセット", use_container_width=True):
                        st.session_state.current_df = None
                        st.session_state.inferred_schema = []
                        st.session_state.table_name = ""
                        st.rerun()

    except Exception as e:
        st.error(f"❌ ファイル読み込みエラー: {str(e)}")

else:
    st.info("👆 CSVファイルをアップロードしてください")

st.markdown("---")
st.markdown("**📊 Streamlitデータアプリ | データ取込 - ©Snowflake合同会社**") 
