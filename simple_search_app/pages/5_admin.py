# =========================================================
# Snowflakeデータ操作アプリケーション
# 保守・運用ページ
# =========================================================
# Created by kdaigo
# 最終更新: 2025/09/24
# =========================================================
import streamlit as st
import json
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide", page_title="🔧 保守・運用", page_icon="🔧")

@st.cache_resource
def get_snowflake_session():
    return get_active_session()

session = get_snowflake_session()

# お知らせはデータベーステーブルで管理（setup SQLで事前作成済み）

# お知らせ管理関数（DB版）
def load_all_announcements():
    """全てのお知らせをデータベースから取得"""
    try:
        result = session.sql("""
        SELECT * FROM application_db.application_schema.ANNOUNCEMENTS 
        ORDER BY priority, created_at DESC
        """).collect()
        return [row.as_dict() for row in result]
    except:
        return []

def save_announcement(announcement_data: dict):
    """お知らせをデータベースに保存"""
    try:
        session.sql("""
        INSERT INTO application_db.application_schema.ANNOUNCEMENTS (
            announcement_id, announcement_type, title, message, 
            start_date, end_date, priority, show_flag
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, params=[
            announcement_data['id'],
            announcement_data['type'],
            announcement_data['title'],
            announcement_data['message'],
            announcement_data['start_date'],
            announcement_data['end_date'],
            announcement_data['priority'],
            announcement_data['show']
        ]).collect()
        return True
    except Exception as e:
        st.error(f"保存エラー: {str(e)}")
        return False

def update_announcement(announcement_id: str, **kwargs):
    """お知らせを更新する"""
    try:
        # 更新可能なフィールドのマッピング
        field_mapping = {
            'title': 'title',
            'message': 'message',
            'type': 'announcement_type',
            'priority': 'priority',
            'start_date': 'start_date',
            'end_date': 'end_date',
            'show': 'show_flag'
        }
        
        update_parts = []
        params = []
        
        for key, value in kwargs.items():
            if key in field_mapping:
                update_parts.append(f"{field_mapping[key]} = ?")
                params.append(value)
        
        if update_parts:
            update_parts.append("updated_at = CURRENT_TIMESTAMP()")
            params.append(announcement_id)
            
            sql = f"""
            UPDATE application_db.application_schema.ANNOUNCEMENTS 
            SET {', '.join(update_parts)}
            WHERE announcement_id = ?
            """
            session.sql(sql, params=params).collect()
            return True
        return False
    except Exception as e:
        st.error(f"更新エラー: {str(e)}")
        return False

def delete_announcement(announcement_id: str):
    """お知らせを削除する"""
    try:
        session.sql("""
        DELETE FROM application_db.application_schema.ANNOUNCEMENTS 
        WHERE announcement_id = ?
        """, params=[announcement_id]).collect()
        return True
    except Exception as e:
        st.error(f"削除エラー: {str(e)}")
        return False

def toggle_announcement(announcement_id: str):
    """お知らせの表示/非表示を切り替える"""
    try:
        # 現在の状態を取得
        result = session.sql("""
        SELECT show_flag FROM application_db.application_schema.ANNOUNCEMENTS 
        WHERE announcement_id = ?
        """, params=[announcement_id]).collect()
        
        if result:
            current_show = result[0]['SHOW_FLAG']
            new_show = not current_show
            
            session.sql("""
            UPDATE application_db.application_schema.ANNOUNCEMENTS 
            SET show_flag = ?, updated_at = CURRENT_TIMESTAMP()
            WHERE announcement_id = ?
            """, params=[new_show, announcement_id]).collect()
            
            return new_show
        return False
    except Exception as e:
        st.error(f"切り替えエラー: {str(e)}")
        return False

def validate_date_range(start_date: str, end_date: str) -> tuple[bool, str]:
    """日付範囲の妥当性をチェック"""
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start > end:
            return False, "開始日は終了日より前の日付を指定してください。"
        
        # 過去の日付チェック（警告レベル）
        today = datetime.now()
        if end < today:
            return True, "終了日が過去の日付です。表示されない可能性があります。"
            
        return True, ""
    except ValueError:
        return False, "日付形式が正しくありません。"

def render_new_announcement_form():
    """新規お知らせ作成フォームを表示"""
    with st.expander("➕ 新しいお知らせを作成", expanded=False):
        st.markdown("### 新規お知らせ作成")
        
        with st.form("new_announcement_form"):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                new_title = st.text_input("タイトル", placeholder="例：🆕 新機能リリース")
                new_message = st.text_area(
                    "メッセージ", 
                    placeholder="お知らせの詳細内容を入力してください...",
                    height=100
                )
            
            with col2:
                new_type = st.selectbox(
                    "お知らせの種類",
                    options=["info", "success", "warning", "error"],
                    format_func=lambda x: {
                        "info": "📘 情報",
                        "success": "✅ 成功/新機能",
                        "warning": "⚠️ 警告/注意",
                        "error": "❌ エラー/重要"
                    }[x]
                )
                
                new_priority = st.number_input(
                    "優先度 (1-3)", 
                    min_value=1, 
                    max_value=3, 
                    value=2,
                    help="1: 高 | 2: 中 | 3: 低 (数字が小さいほど上位に表示)"
                )
            
            col3, col4 = st.columns(2)
            with col3:
                new_start_date = st.date_input(
                    "表示開始日",
                    value=datetime.now().date()
                )
            
            with col4:
                new_end_date = st.date_input(
                    "表示終了日",
                    value=(datetime.now() + timedelta(days=30)).date()
                )
            
            submitted = st.form_submit_button("📝 お知らせを作成", type="primary", use_container_width=True)
            
            if submitted:
                if not new_title.strip():
                    st.error("タイトルを入力してください。")
                elif not new_message.strip():
                    st.error("メッセージを入力してください。")
                else:
                    # 日付検証
                    is_valid, error_msg = validate_date_range(
                        new_start_date.strftime("%Y-%m-%d"), 
                        new_end_date.strftime("%Y-%m-%d")
                    )
                    
                    if not is_valid:
                        st.error(error_msg)
                    else:
                        # お知らせを作成
                        announcement_data = {
                            'id': f"announcement_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                            'type': new_type,
                            'title': new_title,
                            'message': new_message,
                            'start_date': new_start_date.strftime("%Y-%m-%d"),
                            'end_date': new_end_date.strftime("%Y-%m-%d"),
                            'priority': new_priority,
                            'show': True
                        }
                        
                        if save_announcement(announcement_data):
                            st.success(f"✅ お知らせ「{new_title}」を作成しました！")
                            if error_msg:  # 警告がある場合
                                st.warning(error_msg)
                            st.rerun()

def render_announcement_list():
    """既存お知らせ一覧を表示"""
    st.markdown("### 📋 既存お知らせ一覧")
    
    announcements = load_all_announcements()
    if announcements:
        # ソート用の選択肢
        col1, col2 = st.columns([1, 1])
        with col1:
            sort_by = st.selectbox(
                "並び順 *お知らせ一覧上での並び替えでメイン画面では常に[優先度順]で表示されます",
                options=["priority", "start_date", "type"],
                format_func=lambda x: {
                    "priority": "優先度順",
                    "start_date": "開始日順", 
                    "type": "種類順"
                }[x]
            )
        
        with col2:
            show_filter = st.selectbox(
                "表示フィルター",
                options=["all", "active", "inactive", "expired", "scheduled"],
                format_func=lambda x: {
                    "all": "すべて",
                    "active": "実際に表示中のみ",
                    "inactive": "非表示のみ",
                    "expired": "期限切れのみ",
                    "scheduled": "開始日前のみ"
                }[x]
            )
        
        # フィルタリングとソート（期日チェック込み）
        filtered_announcements = announcements.copy()
        
        if show_filter == "active":
            filtered_announcements = [ann for ann in filtered_announcements if is_announcement_currently_active(ann)[0]]
        elif show_filter == "inactive":
            filtered_announcements = [ann for ann in filtered_announcements if not is_announcement_currently_active(ann)[0]]
        elif show_filter == "expired":
            filtered_announcements = [ann for ann in filtered_announcements if is_announcement_currently_active(ann)[1] == "期限切れ"]
        elif show_filter == "scheduled":
            filtered_announcements = [ann for ann in filtered_announcements if is_announcement_currently_active(ann)[1] == "開始日前"]
        
        # ソート（DB版のカラム名に対応）
        if sort_by == "priority":
            filtered_announcements.sort(key=lambda x: x["PRIORITY"])
        elif sort_by == "start_date":
            filtered_announcements.sort(key=lambda x: x["START_DATE"], reverse=True)
        elif sort_by == "type":
            filtered_announcements.sort(key=lambda x: x["ANNOUNCEMENT_TYPE"])
        
        # お知らせカード表示
        render_announcement_cards(filtered_announcements)
        
        # 統計情報
        render_announcement_stats()
    else:
        st.info("お知らせが登録されていません。新しいお知らせを作成してください。")

def is_announcement_currently_active(ann: dict) -> tuple[bool, str]:
    """お知らせが現在アクティブかどうかを判定（期日チェック込み）"""
    current_date = datetime.now().date()
    
    # showフラグがFalseの場合（DB版のカラム名に対応）
    if not ann["SHOW_FLAG"]:
        return False, "手動で非表示"
    
    try:
        # 日付をdateオブジェクトに変換
        if isinstance(ann["START_DATE"], str):
            start_date = datetime.strptime(ann["START_DATE"], "%Y-%m-%d").date()
        else:
            start_date = ann["START_DATE"]
            
        if isinstance(ann["END_DATE"], str):
            end_date = datetime.strptime(ann["END_DATE"], "%Y-%m-%d").date()
        else:
            end_date = ann["END_DATE"]
        
        # 期日チェック
        if start_date > current_date:
            return False, "開始日前"
        elif end_date < current_date:
            return False, "期限切れ"
        else:
            return True, "表示中"
    except (ValueError, TypeError):
        # 日付変換エラーの場合は非表示扱い
        return False, "日付エラー"

def render_announcement_cards(announcements):
    """お知らせカードを表示"""
    for i, ann in enumerate(announcements):
        # カードスタイルでの表示
        card_color = {
            "info": "#e3f2fd",
            "success": "#e8f5e8", 
            "warning": "#fff3e0",
            "error": "#ffebee"
        }.get(ann["ANNOUNCEMENT_TYPE"], "#f5f5f5")
        
        # 実際の表示状態を判定（期日チェック込み）
        is_active, status_reason = is_announcement_currently_active(ann)
        
        if is_active:
            status_icon = "👁️"
            status_text = "表示中"
            status_color = "#4caf50"  # 緑色
        else:
            status_icon = "👁️‍🗨️"
            status_text = f"非表示 ({status_reason})"
            status_color = "#f44336"  # 赤色
        
        type_icon = {
            "info": "📘",
            "success": "✅",
            "warning": "⚠️", 
            "error": "❌"
        }.get(ann["ANNOUNCEMENT_TYPE"], "📝")
        
        with st.container():
            st.markdown(f"""
            <div style="border-left: 4px solid #1f77b4; padding: 1rem; margin: 0.5rem 0; background-color: {card_color}; border-radius: 5px;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <h4>{type_icon} {ann['TITLE']}</h4>
                    <span style="color: {status_color}; font-weight: bold;">優先度: {ann['PRIORITY']} | {status_icon} {status_text}</span>
                </div>
                <p style="margin: 0.5rem 0;">{ann['MESSAGE'][:100]}{'...' if len(ann['MESSAGE']) > 100 else ''}</p>
                <small style="color: #666;">表示期間: {ann['START_DATE']} ～ {ann['END_DATE']}</small>
            </div>
            """, unsafe_allow_html=True)
            
            # 操作ボタン
            render_announcement_buttons(ann)
            
            # 削除確認ダイアログ
            render_delete_confirmation(ann)
            
            # 編集フォーム
            render_edit_form(ann)
            
            st.markdown("---")

def render_announcement_buttons(ann):
    """お知らせ操作ボタンを表示"""
    col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 2])
    
    with col1:
        if st.button("👁️", key=f"toggle_{ann['ANNOUNCEMENT_ID']}", help="表示/非表示切り替え"):
            new_status = toggle_announcement(ann['ANNOUNCEMENT_ID'])
            status_text = "表示" if new_status else "非表示"
            st.success(f"「{ann['TITLE']}」を{status_text}に切り替えました。")
            st.rerun()
    
    with col2:
        if st.button("✏️", key=f"edit_{ann['ANNOUNCEMENT_ID']}", help="編集"):
            st.session_state[f"editing_{ann['ANNOUNCEMENT_ID']}"] = True
            st.rerun()
    
    with col3:
        if st.button("🗑️", key=f"delete_{ann['ANNOUNCEMENT_ID']}", help="削除"):
            st.session_state[f"confirm_delete_{ann['ANNOUNCEMENT_ID']}"] = True
            st.rerun()
    
    with col4:
        if st.button("📋", key=f"copy_{ann['ANNOUNCEMENT_ID']}", help="複製"):
            # 複製機能
            copy_data = {
                'id': f"announcement_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                'type': ann['ANNOUNCEMENT_TYPE'],
                'title': f"{ann['TITLE']} (コピー)",
                'message': ann['MESSAGE'],
                'start_date': ann['START_DATE'],
                'end_date': ann['END_DATE'],
                'priority': ann['PRIORITY'],
                'show': True
            }
            if save_announcement(copy_data):
                st.success(f"「{ann['TITLE']}」を複製しました。")
                st.rerun()

def render_delete_confirmation(ann):
    """削除確認ダイアログを表示"""
    if st.session_state.get(f"confirm_delete_{ann['ANNOUNCEMENT_ID']}", False):
        st.warning(f"「{ann['TITLE']}」を削除しますか？この操作は取り消せません。")
        col_yes, col_no = st.columns(2)
        with col_yes:
            if st.button("はい、削除します", key=f"confirm_yes_{ann['ANNOUNCEMENT_ID']}", type="primary"):
                if delete_announcement(ann['ANNOUNCEMENT_ID']):
                    st.success(f"「{ann['TITLE']}」を削除しました。")
                    st.session_state[f"confirm_delete_{ann['ANNOUNCEMENT_ID']}"] = False
                    st.rerun()
        with col_no:
            if st.button("キャンセル", key=f"confirm_no_{ann['ANNOUNCEMENT_ID']}"):
                st.session_state[f"confirm_delete_{ann['ANNOUNCEMENT_ID']}"] = False
                st.rerun()

def render_edit_form(ann):
    """編集フォームを表示"""
    if st.session_state.get(f"editing_{ann['ANNOUNCEMENT_ID']}", False):
        st.markdown("#### ✏️ 編集中")
        with st.form(f"edit_form_{ann['ANNOUNCEMENT_ID']}"):
            edit_title = st.text_input("タイトル", value=ann['TITLE'])
            edit_message = st.text_area("メッセージ", value=ann['MESSAGE'], height=100)
            
            col_type, col_priority = st.columns(2)
            with col_type:
                edit_type = st.selectbox(
                    "お知らせの種類",
                    options=["info", "success", "warning", "error"],
                    index=["info", "success", "warning", "error"].index(ann['ANNOUNCEMENT_TYPE']),
                    format_func=lambda x: {
                        "info": "📘 情報",
                        "success": "✅ 成功/新機能", 
                        "warning": "⚠️ 警告/注意",
                        "error": "❌ エラー/重要"
                    }[x]
                )
            
            with col_priority:
                edit_priority = st.number_input(
                    "優先度", 
                    min_value=1, 
                    max_value=3, 
                    value=min(ann['PRIORITY'], 3),  # 既存データが3を超える場合は3に調整
                    help="1: 高 | 2: 中 | 3: 低"
                )
            
            col_start, col_end = st.columns(2)
            with col_start:
                # 日付データの型に応じて適切に変換
                if isinstance(ann['START_DATE'], str):
                    start_date_value = datetime.strptime(ann['START_DATE'], "%Y-%m-%d").date()
                else:
                    start_date_value = ann['START_DATE']
                
                edit_start_date = st.date_input(
                    "表示開始日",
                    value=start_date_value
                )
            
            with col_end:
                # 日付データの型に応じて適切に変換
                if isinstance(ann['END_DATE'], str):
                    end_date_value = datetime.strptime(ann['END_DATE'], "%Y-%m-%d").date()
                else:
                    end_date_value = ann['END_DATE']
                
                edit_end_date = st.date_input(
                    "表示終了日",
                    value=end_date_value
                )
            
            col_save, col_cancel = st.columns(2)
            with col_save:
                if st.form_submit_button("💾 保存", type="primary"):
                    is_valid, error_msg = validate_date_range(
                        edit_start_date.strftime("%Y-%m-%d"),
                        edit_end_date.strftime("%Y-%m-%d")
                    )
                    
                    if is_valid:
                        if update_announcement(
                            ann['ANNOUNCEMENT_ID'],
                            title=edit_title,
                            message=edit_message,
                            type=edit_type,
                            priority=edit_priority,
                            start_date=edit_start_date.strftime("%Y-%m-%d"),
                            end_date=edit_end_date.strftime("%Y-%m-%d")
                        ):
                            st.success("✅ お知らせを更新しました！")
                            st.session_state[f"editing_{ann['ANNOUNCEMENT_ID']}"] = False
                            if error_msg:
                                st.warning(error_msg)
                            st.rerun()
                    else:
                        st.error(error_msg)
            
            with col_cancel:
                if st.form_submit_button("❌ キャンセル"):
                    st.session_state[f"editing_{ann['ANNOUNCEMENT_ID']}"] = False
                    st.rerun()

def render_announcement_stats():
    """お知らせ統計情報を表示"""
    announcements = load_all_announcements()
    if announcements:
        st.markdown("### 📊 お知らせ統計")
        total_count = len(announcements)
        
        # 期日チェック込みの統計
        active_count = 0
        inactive_count = 0
        expired_count = 0
        scheduled_count = 0
        
        for ann in announcements:
            is_active, status_reason = is_announcement_currently_active(ann)
            if is_active:
                active_count += 1
            else:
                inactive_count += 1
                if status_reason == "期限切れ":
                    expired_count += 1
                elif status_reason == "開始日前":
                    scheduled_count += 1
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("総お知らせ数", total_count)
        with col2:
            st.metric("表示中", active_count, delta=None, help="現在実際に表示されているお知らせ")
        with col3:
            st.metric("非表示", inactive_count, delta=None, help="手動非表示・期限切れ・開始日前を含む")
        with col4:
            if expired_count > 0:
                st.metric("期限切れ", expired_count, delta=None, help="表示期間を過ぎたお知らせ")
            elif scheduled_count > 0:
                st.metric("予約中", scheduled_count, delta=None, help="開始日前のお知らせ")

st.title("🔧 保守・運用")
st.header("システムの保守・運用と管理機能")

# サイドバーメニューは上部ナビゲーションと重複するため削除

# タブでの機能分割
tab1, tab2 = st.tabs(["📢 お知らせ管理", "🔧 その他管理機能"])

with tab1:
    st.subheader("📢 お知らせ管理")
    st.info("ホーム画面に表示されるお知らせを管理できます。追加・編集・削除・表示切り替えが可能です。")
    
    # 新規お知らせ作成セクション
    render_new_announcement_form()
    
    # 既存お知らせ管理セクション
    render_announcement_list()

with tab2:
    st.subheader("🔧 その他の管理機能")
    st.info("この機能は実装の一例です")
    
    # プレースホルダーとして簡単な機能を表示
    with st.expander("🗄️ テーブル情報", expanded=False):
        if st.button("📊 利用可能テーブル一覧を表示"):
            try:
                tables = session.sql("SHOW TABLES").collect()
                if tables:
                    # 一時テーブル（SNOWPARK_TEMP_TABLEで始まるもの）を除外
                    filtered_tables = [table for table in tables if not table['name'].startswith('SNOWPARK_TEMP_TABLE')]
                    table_names = [table['name'] for table in filtered_tables]
                    
                    st.write(f"**利用可能テーブル数**: {len(table_names)}")
                    if len(tables) > len(filtered_tables):
                        st.info(f"一時テーブル {len(tables) - len(filtered_tables)} 件を除外しました")
                    
                    if table_names:
                        st.write("**テーブル一覧**:")
                        for table in table_names[:10]:  # 最初の10個のみ表示
                            st.write(f"- {table}")
                        if len(table_names) > 10:
                            st.write(f"... 他 {len(table_names) - 10}個")
                    else:
                        st.info("利用可能なテーブルがありません（一時テーブルを除く）。")
                else:
                    st.info("利用可能なテーブルがありません。")
            except Exception as e:
                st.error(f"テーブル情報の取得に失敗しました: {str(e)}")

st.markdown("---")
st.markdown("**📊 Streamlitデータアプリ | 保守・運用 - ©Snowflake合同会社**") 
