import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from data.database.db_manager import DatabaseManager
from data.database.models import StockDB

class StockHistoryCrawler:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
        self.max_retries = 20  # 添加最大重試次數
        self.retry_delay = 5   # 添加重試延遲時間
        
    def clean_number(self, value: str) -> float:
        """清理數字字串，移除逗號並轉換為浮點數"""
        if not value or value.strip() == "--":
            return None
        # 處理 'X0.00' 的情況
        if value.strip().startswith('X'):
            return 0
        # 移除逗號後轉換為浮點數
        return float(value.replace(",", ""))

    def fetch_stock_history(self, stock_id: str, stock_name: str, date: datetime):
        """從證交所抓取單一股票某月份的歷史資料"""
        params = {
            "stockNo": stock_id,
            "date": date.strftime("%Y%m%d"),
            "response": "json"
        }
        
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                if retry_count > 0:
                    retry_delay = self.retry_delay * (2 ** (retry_count - 1))  # 指數退避
                    print(f"Retrying {stock_id} {stock_name} (Attempt {retry_count + 1}/{self.max_retries})")
                    print(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)

                response = requests.get(self.base_url, params=params)
                data = response.json()
                
                if data.get("stat") != "OK":
                    print(f"Error fetching data for {stock_id}: {data.get('stat')}")
                    retry_count += 1
                    continue
                
                records = []
                for row in data.get("data", []):
                    # 轉換民國年為西元年
                    date_str = row[0].replace("/", "")
                    year = int(date_str[:3]) + 1911
                    current_date = datetime.strptime(f"{year}{date_str[3:]}", "%Y%m%d")
                    
                    # 處理可能的空值或無效值
                    try:
                        closing_price = self.clean_number(row[6])
                        price_change = self.clean_number(row[7])
                        
                        # 計算漲跌百分比
                        if closing_price is not None and price_change is not None:
                            prev_close = closing_price - price_change
                            change_percent = (price_change / prev_close * 100) if prev_close != 0 else None
                        else:
                            change_percent = None
                        
                        record = (
                            current_date,
                            stock_id,
                            stock_name,                           # 股票名稱
                            int(row[1].replace(",", "")),         # 成交股數
                            int(row[2].replace(",", "")),         # 成交金額
                            self.clean_number(row[3]),            # 開盤價
                            self.clean_number(row[4]),            # 最高價
                            self.clean_number(row[5]),            # 最低價
                            closing_price,                        # 收盤價
                            price_change,                         # 漲跌價差
                            change_percent,                       # 漲跌百分比
                            int(row[8].replace(",", "")),         # 成交筆數
                            None,                                 # 5日均線
                            None,                                 # 10日均線
                            None,                                 # 20日均線
                            None                                  # 60日均線
                        )
                        records.append(record)
                    except (ValueError, IndexError) as e:
                        print(f"Error processing row for {stock_id}: {e}")
                        continue
                        
                return records
                
            except Exception as e:
                print(f"Error fetching data for {stock_id}: {e}")
                retry_count += 1
                if retry_count >= self.max_retries:
                    return []
                
        return []
    
    def calculate_moving_averages(self, stock_id: str):
        """使用 pandas 計算均線"""
        # 獲取股票歷史資料
        data = self.db_manager.conn.execute(
            StockDB.GET_STOCK_HISTORY_FOR_MA,
            [stock_id]
        ).fetchall()
        
        if not data:
            return
        
        # 轉換為 DataFrame
        df = pd.DataFrame(data, columns=['date', 'stock_id', 'closing_price'])

        # 先用前一個有效值填充空值
        df['closing_price'] = df['closing_price'].ffill()
        
        # 計算各期均線
        for period in [5, 10, 20, 60]:
            df[f'ma{period}'] = df['closing_price'].rolling(window=period).mean().round(3)
        
        # 更新資料庫
        updates = []
        for _, row in df.iterrows():
            updates.append((
                row['ma5'],
                row['ma10'],
                row['ma20'],
                row['ma60'],
                stock_id,
                row['date']
            ))
        
        # 批次更新均線值
        self.db_manager.conn.executemany(StockDB.UPDATE_MA_VALUES, updates)
    
    def crawl_followed_stocks_history(self, start_date: datetime, end_date: datetime):
        """爬取所有追蹤股票的歷史資料"""
        followed_stocks = self.db_manager.get_followed_stocks()
        
        # 第一步：抓取所有基本資料
        current_date = start_date
        while current_date <= end_date:
            print(current_date)
            for stock_id, stock_name in followed_stocks:
                # print(f"Fetching {stock_id} {stock_name} for {current_date.strftime('%Y-%m')}")
                
                records = self.fetch_stock_history(stock_id, stock_name, current_date)
                if records:
                    self.db_manager.upsert_daily_data(records)
                
                # 避免請求過於頻繁
                time.sleep(2)
            
            # 移至下個月
            current_date = (current_date.replace(day=1) + timedelta(days=32)).replace(day=1)
        
        # 第二步：計算所有股票的均線
        print("Calculating moving averages...")
        for stock_id, _ in followed_stocks:
            print(f"Calculating moving averages for {stock_id}")
            self.calculate_moving_averages(stock_id)

def main():
    db_manager = DatabaseManager(
        db_path="StockHero.db",
        bucket_name="ian-line-bot-files"
    )
    db_manager.connect()
    
    crawler = StockHistoryCrawler(db_manager)
    start_date = datetime(2024, 7, 1)
    end_date = datetime(2025, 1, 22)
    
    try:
        crawler.crawl_followed_stocks_history(start_date, end_date)
    finally:
        db_manager.close()

if __name__ == "__main__":
    main()