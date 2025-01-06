import streamlit as st
import pandas as pd

def render():
    # 頁面標題
    st.title("股票篩選器 🔍")

    # 建立篩選條件
    col1, col2, col3 = st.columns(3)

    with col1:
        price_range = st.slider(
            "股價範圍",
            min_value=0,
            max_value=3000,
            value=(0, 500)
        )

    with col2:
        pe_range = st.slider(
            "本益比範圍",
            min_value=0,
            max_value=100,
            value=(0, 30)
        )

    with col3:
        volume = st.number_input(
            "最小成交量(張)",
            min_value=0,
            value=1000
        )

    # 產業選擇
    industry = st.multiselect(
        "選擇產業",
        ["半導體", "電子", "金融", "傳統產業", "生技醫療"],
        default=[]
    )

    # 查詢按鈕
    if st.button("開始篩選"):
        example_data = {
            "股票代號": ["2330", "2317", "2454"],
            "股票名稱": ["台積電", "鴻海", "聯發科"],
            "產業": ["半導體", "電子", "半導體"],
            "股價": [500, 100, 800],
            "本益比": [15, 10, 20],
            "成交量": [50000, 30000, 20000]
        }
        df = pd.DataFrame(example_data)
        
        # 套用篩選條件
        mask = (
            (df["股價"].between(price_range[0], price_range[1])) &
            (df["本益比"].between(pe_range[0], pe_range[1])) &
            (df["成交量"] >= volume)
        )
        
        if industry:
            mask = mask & (df["產業"].isin(industry))
        
        filtered_df = df[mask]
        
        # 顯示結果
        st.subheader("篩選結果")
        st.dataframe(filtered_df, use_container_width=True)
        
        # 下載按鈕
        st.download_button(
            label="下載篩選結果",
            data=filtered_df.to_csv(index=False).encode('utf-8-sig'),
            file_name='stock_screening_results.csv',
            mime='text/csv'
        )