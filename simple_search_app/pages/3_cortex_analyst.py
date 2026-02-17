# =========================================================
# Snowflakeデータ操作アプリケーション
# Cortex Analyst分析（一時無効化）
# =========================================================
# Created by kdaigo
# 最終更新: 2025/09/24
# =========================================================
import streamlit as st

# ページ設定
st.set_page_config(layout="wide", page_title="🤖 自然言語分析", page_icon="🤖")

st.title("🤖 自然言語分析")

# =========================================================
# 一時無効化メッセージ
# =========================================================
st.markdown("""
<div style="border: 2px solid #cccccc; border-radius: 10px; padding: 2rem; text-align: center; background-color: #f5f5f5; opacity: 0.8; margin: 2rem 0;">
    <h2 style="color: #999;">⚠️ この機能は現在準備中です</h2>
    <p style="color: #999; font-size: 1.1em;">
        Cortex Analystを利用した自然言語分析機能は、現在一時的に無効化されています。<br>
        セマンティックビューの設定が完了次第、こちらの機能をご利用いただけるようになります。
    </p>
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# =========================================================
# 無効化されたUI要素（グレーアウト表示）
# =========================================================
st.markdown("### 📊 セマンティックビュー設定")
st.selectbox("使用するセマンティックビュー:", ["（セマンティックビュー未設定）"], disabled=True)

st.markdown("### ⚙️ Analyst設定")
col1, col2 = st.columns(2)
with col1:
    st.selectbox("LLMモデル:", ["llama4-maverick", "claude-4-sonnet", "mistral-large2"], disabled=True)
with col2:
    st.checkbox("カスタマイズグラフ表示", value=True, disabled=True)

st.markdown("---")

st.markdown("### 🔍 自然言語データ分析")
st.text_input(
    "💬 データについて質問してください:",
    placeholder="例: 顧客セグメント別の人数と平均年齢を教えて",
    disabled=True
)

st.button("🚀 Cortex Analyst分析", type="primary", use_container_width=True, disabled=True)

st.markdown("---")

st.markdown("### 💡 よくある分析テンプレート")

analysis_templates = [
    "顧客セグメント別の人数と平均年齢を教えて",
    "月別の取引金額と件数の推移を見せて",
    "チャネル別（Web、モバイルアプリ、ATM）の利用状況を比較して",
    "取引種別ごとの合計金額ランキングを作って"
]

col1, col2 = st.columns(2)
for i, question in enumerate(analysis_templates):
    with col1 if i % 2 == 0 else col2:
        st.button(question, key=f"template_{i}", use_container_width=True, disabled=True)

# フッター
st.markdown("---")
st.markdown("**📊 Streamlitデータアプリ | 自然言語分析（準備中） - ©Snowflake合同会社**")
