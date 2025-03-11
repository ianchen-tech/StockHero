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

class RatioUpdater:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.base_url = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d"
        self.max_retries = 8
        self.retry_delay = 5
    
    def clean_number(self, value) -> float:
        """清理數字字串，移除逗號並轉換為浮點數"""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if not value or value.strip() == "--" or value.strip() == "-":
            return None
        return float(value.replace(",", ""))

    def fetch_daily_ratios(self, date: datetime) -> dict:
        """抓取當日所有股票的本益比、淨值比和殖利率資料"""
        retry_count = 0
        
        while retry_count < self.max_retries:
            if retry_count > 0:
                retry_delay = min(60, self.retry_delay * (2 ** (retry_count - 1)))  # 指數退避，最大延遲60秒
                logger.warning(f"Retrying to fetch daily ratios (Attempt {retry_count + 1}/{self.max_retries})")
                logger.warning(f"Waiting {retry_delay} seconds before retry...")
                time.sleep(retry_delay)
            
            params = {
                "date": date.strftime("%Y%m%d"),
                "response": "json"
            }
            
            try:
                response = requests.get(self.base_url, params=params)
                data = response.json()
                
                if data.get("stat") != "OK":
                    logger.error(f"Error fetching data: {data.get('stat')}")
                    retry_count += 1
                    continue
                
                if not data.get("data"):
                    logger.info(f"No trading data available for {date.strftime('%Y-%m-%d')}")
                    return None
                
                # 建立一個字典來存儲所有股票的資料
                ratio_data = {}
                
                for row in data.get("data", []):
                    try:
                        stock_id = row[0]
                        ratio_data[stock_id] = {
                            'pe_ratio': self.clean_number(row[5]),
                            'pb_ratio': self.clean_number(row[6]),
                            'dividend_yield': self.clean_number(row[3])
                        }
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error processing data for stock {row[0]}: {e}")
                        continue
                
                return ratio_data
                
            except requests.exceptions.RequestException as e:
                logger.exception(f"Network error while fetching daily ratio data: {e}")
                retry_count += 1
            except Exception as e:
                logger.exception(f"Unexpected error while fetching daily ratio data: {e}")
                retry_count += 1
        
        logger.error("Max retries reached while fetching daily ratios")
        return None

    def update_ratio_data(self, update_date: datetime = None):
        """更新最新的本益比和淨值比資料"""
        if update_date is None:
            update_date = datetime.now()

        self.db_manager.connect()
        
        try:
            # 使用重試機制獲取所有股票的資料
            all_ratios = self.fetch_daily_ratios(update_date)
            if not all_ratios:
                return False, f"Failed to fetch ratio data for {update_date.strftime('%Y-%m-%d')}"

            # 獲取需要更新的股票清單
            followed_stocks = self.db_manager.get_followed_stocks()
            failed_stocks = []
            updated_stocks = []
            
            # 只更新關注的股票
            for stock_id, stock_name in followed_stocks:
                # 跳過包含英文字母的股票ID
                if any(c.isalpha() for c in stock_id):
                    logger.info(f"Skipping stock with alphabetic ID: {stock_id} {stock_name}")
                    continue
                    
                if stock_id in all_ratios:
                    try:
                        ratios = all_ratios[stock_id]
                        # 更新資料庫
                        self.db_manager.conn.execute("""
                            UPDATE stock_daily 
                            SET pe_ratio = ?, pb_ratio = ?, dividend_yield = ?
                            WHERE stock_id = ? AND date = ?
                        """, [
                            ratios['pe_ratio'],
                            ratios['pb_ratio'],
                            ratios['dividend_yield'],
                            stock_id,
                            update_date.strftime("%Y-%m-%d")
                        ])
                        updated_stocks.append((stock_id, stock_name))
                        logger.info(f"Successfully updated ratio data for {stock_id} {stock_name}")
                    except Exception as e:
                        logger.exception(f"Error updating database for {stock_id}: {e}")
                        failed_stocks.append((stock_id, stock_name))
                else:
                    logger.warning(f"No ratio data found for {stock_id} {stock_name}")
                    failed_stocks.append((stock_id, stock_name))

            self.db_manager.conn.commit()
                
            # 處理最終結果
            if not failed_stocks:
                success_message = (
                    f"Ratio update completed successfully for {update_date.strftime('%Y-%m-%d')}. "
                    f"Updated {len(updated_stocks)} stocks."
                )
                logger.info(success_message)
                return True, success_message
            else:
                failed_message = (
                    f"Update completed with some failures for {update_date.strftime('%Y-%m-%d')}. "
                    f"Updated {len(updated_stocks)} stocks. "
                    f"Failed stocks: {', '.join([f'{id}({name})' for id, name in failed_stocks])}"
                )
                logger.error(failed_message)
                return False, failed_message
            
        except Exception as e:
            error_message = f"Error during ratio update: {e}"
            logger.exception(error_message)
            return False, error_message
        
        finally:
            self.db_manager.close()


if __name__ == "__main__":
    db_manager = DatabaseManager(
        db_path="StockHero.db",
        bucket_name="ian-line-bot-files"
    )
    
    updater = RatioUpdater(db_manager)
    test_date = datetime(2025, 3, 10)
    success, message = updater.update_ratio_data(test_date)
    
