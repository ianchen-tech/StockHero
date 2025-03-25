import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.database.db_manager import DatabaseManager
from data.crawler.stock_update import StockUpdater
from data.crawler.ratio_update import RatioUpdater
from data.analysis.screening import StockScreener
from data.analysis.kd_calculator import KDCalculator
from datetime import datetime
from config.logger import setup_logging

# 設置 logger
logger = setup_logging()

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

        logger.info(f"Starting stock data update for date: {update_date}")
        
        # 初始化資料庫管理器
        db_manager = DatabaseManager(
            db_path="StockHero.db",
            bucket_name="ian-line-bot-files"
        )
        
        # 執行更新
        updater = StockUpdater(db_manager)
        update_success, update_message = updater.update_daily_data(update_date)
        
        if update_success:
            logger.info(f"Stock data update completed successfully: {update_message}")
            
            # 執行本益比、淨值比和殖利率更新
            ratio_updater = RatioUpdater(db_manager)
            ratio_success, ratio_message = ratio_updater.update_ratio_data(update_date)
            
            if not ratio_success:
                logger.warning(f"Ratio update warning: {ratio_message}")
            else:
                logger.info(f"Ratio update completed successfully: {ratio_message}")
            
            # 執行 KD 值計算
            kd_calculator = KDCalculator(db_manager)
            kd_success, kd_message = kd_calculator.calculate_kd_values(update_date)
            
            if not kd_success:
                logger.warning(f"KD calculation warning: {kd_message}")
            else:
                logger.info(f"KD calculation completed successfully: {kd_message}")
            
            # 執行條件篩選
            screener = StockScreener(db_manager)
            screen_success, screen_message = screener.screen_stocks(update_date)
            
            if screen_success:
                logger.info(f"Stock screening completed successfully: {screen_message}")
                return True, "Update and screening completed successfully"
            else:
                logger.error(f"Stock screening failed: {screen_message}")
                return False, f"Update succeeded but screening failed: {screen_message}"
        else:
            logger.error(f"Stock data update failed: {update_message}")
            return False, update_message
            
    except Exception as e:
        error_message = f"Update failed: {str(e)}"
        logger.exception("Unexpected error during update")
        return False, error_message

if __name__ == "__main__":
    update_date = datetime.strptime("2025-03-24", "%Y-%m-%d")
    success, message = update_stock_data(update_date)
    # print(message)