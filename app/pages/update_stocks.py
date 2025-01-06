import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import streamlit as st
from datetime import datetime
from jobs.daily_update import update_stock_data

# 隱藏所有 Streamlit 預設的 UI 元素
st.set_page_config(
    page_title="Stock Update",
    initial_sidebar_state="collapsed"
)

hide_streamlit_style = """
<style>
    #root > div:nth-child(1) > div > div > div > div > section > div {visibility: hidden;}
    #root > div:nth-child(1) > div > div > div > div {padding-top: 0rem;}
</style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# 從 URL 獲取日期參數
date_str = st.query_params.get('date', None)

try:
    # 如果有提供日期參數，轉換成 datetime 物件
    if date_str:
        update_date = datetime.strptime(date_str, '%Y%m%d')
    else:
        update_date = datetime.now()
    
    # 執行更新
    success, message = update_stock_data(update_date)

    # 回傳純文字結果
    st.text(message)
    
except ValueError as e:
    st.text(f"Invalid date format. Please use YYYYMMDD format. Error: {str(e)}")
except Exception as e:
    st.text(f"Error: {str(e)}")