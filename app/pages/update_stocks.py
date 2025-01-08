import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import streamlit as st
from datetime import datetime
from jobs.daily_update import update_stock_data
import threading
from config.logger import setup_logging

# 設置 logger
logger = setup_logging()

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

def run_update(update_date):
    try:
        success, message = update_stock_data(update_date)
        logger.info(f"Update completed: {message}")
    except Exception as e:
        logger.exception(f"Update failed: {str(e)}")

# 從 URL 獲取日期參數
date_str = st.query_params.get('date', None)

try:
    # 如果有提供日期參數，轉換成 datetime 物件
    if date_str:
        update_date = datetime.strptime(date_str, '%Y%m%d')
    else:
        update_date = datetime.now()
    
    # 在背景執行更新
    thread = threading.Thread(target=run_update, args=(update_date,))
    thread.start()

    # 立即回傳成功訊息
    st.text("Update process started successfully")
    
except ValueError as e:
    st.text(f"Invalid date format. Please use YYYYMMDD format. Error: {str(e)}")
except Exception as e:
    st.text(f"Error: {str(e)}")