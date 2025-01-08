import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from data.database.db_manager import DatabaseManager
from data.database.models import StockDB
from config.logger import setup_logging

# 設置 logger
logger = setup_logging()

class StockUpdater:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
        self.max_retries = 3
        self.retry_delay = 5
    
    def clean_number(self, value: str) -> float:
        """清理數字字串，移除逗號並轉換為浮點數"""
        if not value or value.strip() == "--":
            return None
        if value.strip().startswith('X'):
            return 0
        return float(value.replace(",", ""))

    def check_market_open(self, date: datetime) -> bool:
        """檢查指定日期是否有交易資料"""
        params = {
            "stockNo": "2330",
            "date": date.strftime("%Y%m%d"),
            "response": "json"
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            data = response.json()
            
            if data.get("stat") != "OK":
                logger.info(f"Market status check: No data available (stat: {data.get('stat')})")
                return False
            
            target_date = date.strftime("%Y/%m/%d")
            for row in data.get("data", []):
                date_str = row[0]
                year = int(date_str.split('/')[0]) + 1911
                converted_date = f"{year}/{date_str[4:]}"
                if converted_date == target_date:
                    logger.info(f"Market was open on {date.strftime('%Y-%m-%d')}")
                    return True
            
            logger.info(f"Market was closed on {date.strftime('%Y-%m-%d')}")
            return False
            
        except Exception as e:
            logger.exception(f"Error checking market status: {e}")
            return False

    def fetch_daily_data(self, stock_id: str, stock_name: str, date: datetime):
        """抓取單一股票單日的資料"""
        params = {
            "stockNo": stock_id,
            "date": date.strftime("%Y%m%d"),
            "response": "json"
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            data = response.json()
            
            if data.get("stat") != "OK":
                logger.error(f"Error fetching data for {stock_id}: {data.get('stat')}")
                return None
            
            target_date = date.strftime("%Y/%m/%d")
            for row in data.get("data", []):
                # 轉換民國年為西元年
                date_str = row[0]
                year = int(date_str.split('/')[0]) + 1911
                converted_date = f"{year}/{date_str[4:]}"
                
                if converted_date == target_date:
                    try:
                        closing_price = self.clean_number(row[6])
                        price_change = self.clean_number(row[7])
                        
                        # 計算漲跌百分比
                        if closing_price is not None and price_change is not None:
                            prev_close = closing_price - price_change
                            change_percent = (price_change / prev_close * 100) if prev_close != 0 else None
                        else:
                            change_percent = None
                        
                        return (
                            date,
                            stock_id,
                            stock_name,
                            int(row[1].replace(",", "")),     # 成交股數
                            int(row[2].replace(",", "")),     # 成交金額
                            self.clean_number(row[3]),        # 開盤價
                            self.clean_number(row[4]),        # 最高價
                            self.clean_number(row[5]),        # 最低價
                            closing_price,                    # 收盤價
                            price_change,                     # 漲跌價差
                            change_percent,                   # 漲跌百分比
                            int(row[8].replace(",", "")),     # 成交筆數
                            None, None, None, None            # 均線值先設為 None
                        )
                    except (ValueError, IndexError) as e:
                        logger.exception(f"Error processing data for {stock_id}: {e}")
                        return None
            
            return None
            
        except Exception as e:
            logger.exception(f"Error fetching data for {stock_id}: {e}")
            return None

    def calculate_moving_averages(self, stock_id: str):
        """計算均線，只使用最近的資料"""
        data = self.db_manager.conn.execute(
            StockDB.GET_RECENT_STOCK_DATA_FOR_MA,
            [stock_id]
        ).fetchall()
        
        if not data:
            return
        
        # 將資料轉換為 DataFrame 並反轉順序（使最新的在最後）
        df = pd.DataFrame(data, columns=['date', 'stock_id', 'closing_price'])
        df = df.iloc[::-1].reset_index(drop=True)
        
        # 計算各期均線
        ma_values = {}
        for period in [5, 10, 20, 60]:
            if len(df) >= period:  # 確保有足夠的資料計算均線
                ma_values[f'ma{period}'] = df['closing_price'].rolling(window=period).mean().iloc[-1].round(3)
            else:
                ma_values[f'ma{period}'] = None
        
        # 更新最新的均線值
        self.db_manager.conn.execute(
            StockDB.UPDATE_MA_VALUES,
            [
                ma_values['ma5'],
                ma_values['ma10'],
                ma_values['ma20'],
                ma_values['ma60'],
                stock_id,
                df.iloc[-1]['date']  # 最新日期
            ]
        )
    
    def update_daily_data(self, update_date: datetime = None):
        """更新最新的股票資料"""
        if update_date is None:
            update_date = datetime.now()

        if not self.check_market_open(update_date):
            logger.info(f"Market was closed on {update_date.strftime('%Y-%m-%d')}")
            return False, f"No trading data available for {update_date.strftime('%Y-%m-%d')}"

        self.db_manager.connect()
        
        try:
            followed_stocks = self.db_manager.get_followed_stocks()
            failed_stocks = []  # 記錄更新失敗的股票
            
            for stock_id, stock_name in followed_stocks:

                retry_count = 0
                success = False

                while retry_count < self.max_retries and not success:
                    if retry_count > 0:
                        retry_delay = self.retry_delay * (2 ** (retry_count - 1))  # 指數退避
                        logger.warning(f"Retrying {stock_id} {stock_name} (Attempt {retry_count + 1}/{self.max_retries})")
                        logger.warning(f"Waiting {retry_delay} seconds before retry...")
                        time.sleep(retry_delay)
                
                    # 抓取當天資料
                    daily_data = self.fetch_daily_data(stock_id, stock_name, update_date)
                    
                    if daily_data:
                        try:
                            # 插入或更新資料
                            self.db_manager.upsert_daily_data([daily_data])
                            # 重新計算均線
                            self.calculate_moving_averages(stock_id)
                            success = True
                            logger.info(f"Successfully updated {stock_id} {stock_name}")
                        except Exception as e:
                            logger.exception(f"Error updating database for {stock_id}: {e}")
                            retry_count += 1
                    else:
                        retry_count += 1

                if not success:
                    failed_stocks.append((stock_id, stock_name))
                
                time.sleep(2)  # 正常的請求間隔
                
            # 處理最終結果
            if not failed_stocks:
                success_message = f"Daily update completed successfully for {update_date.strftime('%Y-%m-%d')}"
                logger.info(success_message)
                return True, success_message
            else:
                failed_message = f"Update completed with some failures for {update_date.strftime('%Y-%m-%d')}. "
                failed_message += f"Failed stocks: {', '.join([f'{id}({name})' for id, name in failed_stocks])}"
                logger.error(failed_message)
                return False, failed_message
            
        except Exception as e:
            error_message = f"Error during daily update: {e}"
            logger.exception(error_message)
            return False, error_message
        
        finally:
            self.db_manager.close()

def main():
    """測試程式"""
    db_manager = DatabaseManager(
        db_path="StockHero.db",
        bucket_name="ian-line-bot-files"
    )
    
    updater = StockUpdater(db_manager)
    test_date = datetime(2025, 1, 6)
    success, message = updater.update_daily_data(test_date)
    
    print(f"Test result: {'Success' if success else 'Failed'}")
    print(f"Message: {message}")

if __name__ == "__main__":
    main()