import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import time
import logging
import requests
import pandas as pd
from io import StringIO
from data.database.db_manager import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockInfoCrawler:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        # 定義要追蹤的產業列表
        self.target_industries = [
        # 電子科技相關
        '半導體業',
        '電子零組件業',
        '電腦及週邊設備業',
        '光電業',
        '通信網路業',
        '電子通路業',
        '資訊服務業',
        '電子工業',
        '其他電子業',
        
        # 生技醫療相關
        '生技醫療業',
        
        # 傳統產業
        '水泥工業',
        '食品工業',
        '塑膠工業',
        '電機機械',
        '電器電纜',
        '化學生技醫療',
        '化學工業',
        '橡膠工業',
        '玻璃陶瓷',
        '造紙工業',
        '鋼鐵工業',
        '汽車工業',
        '建材營造業',
        '油電燃氣業',
        
        # 其他產業
        '航運業',
        '貿易百貨業',
        '運動休閒',
        '居家生活',
        '綠能環保',
        '數位雲端',
        '其他業'
        ]

    def crawl_stock_info(self):
        """爬取上市公司產業類別資訊"""
        try:
            # 台灣證券交易所上市公司產業類別表
            url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
            response = requests.get(url, headers=self.headers)
            response.encoding = 'ms950'  # 設定編碼為 Big5
            
            # 使用 pandas 讀取 HTML 表格
            dfs = pd.read_html(StringIO(response.text))
            
            # 第一個表格包含我們需要的資訊
            df = dfs[0]
            df.columns = df.iloc[0]  # 將第一行設為標題
            df = df.iloc[1:]  # 移除第一行
            
            # 處理資料
            for _, row in df.iterrows():
                # 從"有價證券代號及名稱"欄位分離代號和名稱
                if pd.notna(row['有價證券代號及名稱']) and pd.notna(row['產業別']):
                    parts = str(row['有價證券代號及名稱']).strip().split('\u3000', 1)
                    if len(parts) == 2:
                        stock_id = parts[0]
                        stock_name = parts[1]
                        industry = str(row['產業別']).strip() if pd.notna(row['產業別']) else ''
                        market_type = str(row['市場別']).strip() if pd.notna(row['市場別']) else ''

                        # 檢查是否為要追蹤的產業
                        should_follow = industry in self.target_industries
                        
                        # 更新資料庫
                        self.db_manager.upsert_stock_info(
                            stock_id=stock_id,
                            stock_name=stock_name,
                            industry=industry,
                            follow=should_follow,
                            market_type=market_type,
                            source='TWSE'
                        )
                        follow_status = "追蹤" if should_follow else "不追蹤"
                        logger.info(f"已更新股票資訊: {stock_id} {stock_name} - {industry} ({follow_status})")
            
            # 加入延遲以避免請求過於頻繁
            time.sleep(1)
            
            logger.info("股票資訊更新完成")
            
        except Exception as e:
            logger.error(f"爬取股票資訊時發生錯誤: {str(e)}")
            raise

def main():
    # 初始化資料庫管理器
    db_manager = DatabaseManager(
        db_path="StockHero.db",
        bucket_name="ian-line-bot-files"
    )
    
    try:
        # 連接資料庫
        db_manager.connect()
        
        # 建立爬蟲實例並執行爬取
        crawler = StockInfoCrawler(db_manager)
        crawler.crawl_stock_info()
        
    finally:
        # 確保資料庫連接被正確關閉
        db_manager.close()

if __name__ == "__main__":
    main()