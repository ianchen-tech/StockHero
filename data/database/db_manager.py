import os
import duckdb
from google.cloud import storage
from datetime import datetime
from .models import StockDB

class DatabaseManager:
    def __init__(self, db_path: str, bucket_name: str):
        self.db_path = db_path
        self.bucket_name = bucket_name
        self.conn = None
        self.storage_client = storage.Client()
        
    def connect(self):
        """建立資料庫連接"""
        if not os.path.exists(self.db_path):
            self._download_db_from_gcs()
            
        self.conn = duckdb.connect(self.db_path)
        # 建立資料表
        self.conn.execute(StockDB.CREATE_STOCK_DAILY_TABLE)
        self.conn.execute(StockDB.CREATE_STOCK_INFO_TABLE)
    
    def close(self):
        """關閉資料庫連接"""
        if self.conn:
            self.conn.close()
            if not os.path.exists(self.db_path):
                self._upload_db_to_gcs()
    
    def _download_db_from_gcs(self):
        """從 Cloud Storage 下載資料庫檔案"""
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob('StockHero.db')
        blob.download_to_filename(self.db_path)
    
    def _upload_db_to_gcs(self):
        """上傳資料庫檔案到 Cloud Storage"""
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob('StockHero.db')
        blob.upload_from_filename(self.db_path)
    
    def upsert_stock_info(self, stock_id: str, stock_name: str, industry: str, follow: bool, market_type: str, source: str):
        """寫入股票基本資料"""
        now = datetime.now()
        self.conn.execute(
            StockDB.UPSERT_STOCK_INFO,
            [stock_id, stock_name, industry, follow, market_type, source, now, now]
        )

    def upsert_daily_data(self, records: list):
        """寫入每日股票資料"""
        self.conn.executemany(StockDB.UPSERT_DAILY_DATA, records)

    def get_followed_stocks(self):
        """獲取所有追蹤的股票清單"""
        return self.conn.execute(StockDB.GET_FOLLOWED_STOCKS).fetchall()
