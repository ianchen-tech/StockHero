import logging
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
import os

def setup_logging():
    """設置 logging 系統"""
    # 檢查是否在 Cloud Run 環境
    is_cloud_run = os.getenv('K_SERVICE') is not None

    # 創建 logger
    logger = logging.getLogger('StockHero')

    # 如果 logger 已經有 handlers，直接返回
    if logger.handlers:
        return logger

    # 設置級別
    logger.setLevel(logging.INFO)

    # 設置格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if is_cloud_run:
        # Cloud Run 環境：使用 Cloud Logging
        client = google.cloud.logging.Client()
        cloud_handler = CloudLoggingHandler(client, name="stockhero_logs")
        cloud_handler.setFormatter(formatter)
        logger.addHandler(cloud_handler)
    else:
        # 本地環境：輸出到控制台
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger