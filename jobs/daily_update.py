import os
from data.database.db_manager import DatabaseManager
from data.crawler.stock_update import StockUpdater
from datetime import datetime

def update_stock_data(update_date: datetime = None):
    """
    執行股票資料更新
    Args:
        update_date: 指定要更新的日期，如果不指定則使用當天
    """
    try:
        # 如果沒有指定日期，使用當天
        if update_date is None:
            update_date = datetime.now()
        
        # 初始化資料庫管理器
        db_manager = DatabaseManager(
            db_path="StockHero.db",
            bucket_name="ian-line-bot-files"
        )
        
        # 執行更新
        updater = StockUpdater(db_manager)
        return updater.update_daily_data(update_date)
        
    except Exception as e:
        error_message = f"Update failed: {str(e)}"
        print(error_message)
        return False, error_message

if __name__ == "__main__":
    success, message = update_stock_data()
    print(message)