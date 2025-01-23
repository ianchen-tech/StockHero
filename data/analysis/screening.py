import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import pandas as pd
import json
from datetime import datetime
from data.database.db_manager import DatabaseManager
from data.database.models import StockDB
from config.logger import setup_logging

# 設置 logger
logger = setup_logging()

class StockScreener:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        
    def screen_stocks(self, update_date: datetime = None):
        """
        對所有追蹤的股票進行條件篩選
        Args:
            update_date: 指定要篩選的日期，如果不指定則使用當天
        Returns:
            tuple: (success, message)
        """
        if update_date is None:
            update_date = datetime.now()
            
        self.db_manager.connect()
        
        try:
            # 取得最新兩天的資料
            data = self.db_manager.conn.execute(
                StockDB.GET_LATEST_TWO_DAYS_DATA
            ).fetchall()
            
            if not data:
                return False, "No data available for screening"
                
            # 轉換為 DataFrame 進行處理
            df = pd.DataFrame(
                data,
                columns=['date', 'stock_id', 'trade_volume', 'closing_price', 
                        'ma5', 'ma10', 'ma20', 'ma60']
            )
            
            # 進行條件篩選
            results = {}
            success_count = 0
            failed_stocks = []
            
            for stock_id in df['stock_id'].unique():
                try:
                    stock_data = df[df['stock_id'] == stock_id].sort_values('date', ascending=False)
                    
                    if len(stock_data) < 2:
                        logger.warning(f"Insufficient data for {stock_id}")
                        failed_stocks.append(stock_id)
                        continue
                        
                    latest = stock_data.iloc[0]
                    prev = stock_data.iloc[1]
                    
                    conditions = {
                        "volume_increase": bool(latest['trade_volume'] >= prev['trade_volume']),
                        "above_ma5": bool(latest['closing_price'] >= latest['ma5'] if pd.notna(latest['ma5']) else False),
                        "above_ma10": bool(latest['closing_price'] >= latest['ma10'] if pd.notna(latest['ma10']) else False),
                        "above_ma20": bool(latest['closing_price'] >= latest['ma20'] if pd.notna(latest['ma20']) else False),
                        "above_ma60": bool(latest['closing_price'] >= latest['ma60'] if pd.notna(latest['ma60']) else False)
                    }
                    
                    results[stock_id] = json.dumps(conditions)
                    success_count += 1
                    
                except Exception as e:
                    logger.exception(f"Error processing conditions for {stock_id}: {e}")
                    failed_stocks.append(stock_id)
            
            # 更新資料庫
            if results:
                self.db_manager.update_stock_conditions(results)
                
            # 準備回傳訊息
            if not failed_stocks:
                success_message = (f"Screening completed successfully for {success_count} stocks "
                                 f"on {update_date.strftime('%Y-%m-%d')}")
                logger.info(success_message)
                return True, success_message
            else:
                failed_message = (f"Screening completed with {len(failed_stocks)} failures. "
                                f"Failed stocks: {', '.join(failed_stocks)}")
                logger.warning(failed_message)
                return False, failed_message
                
        except Exception as e:
            error_message = f"Error during screening: {str(e)}"
            logger.exception(error_message)
            return False, error_message
            
        finally:
            self.db_manager.close()


if __name__ == "__main__":

    db_manager = DatabaseManager(
        db_path="StockHero.db",
        bucket_name="ian-line-bot-files"
    )
    
    screener = StockScreener(db_manager)
    test_date = datetime.now()
    success, message = screener.screen_stocks(test_date)
    
    print(f"Test result: {'Success' if success else 'Failed'}")
    print(f"Message: {message}")
