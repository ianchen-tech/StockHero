import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from data.database.db_manager import DatabaseManager
from config.logger import setup_logging

# 設置 logger
logger = setup_logging()

class KDCalculator:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.period = 9  # KD 指標的預設週期為 9 天
        self.k_smoothing = 3  # K 值平滑因子
        self.d_smoothing = 3  # D 值平滑因子
    
    def calculate_kd_values(self, update_date: datetime = None):
        """
        計算並更新股票的 KD 值
        Args:
            update_date: 指定要更新的日期，如果不指定則使用當天
        Returns:
            (bool, str): 成功與否及相關訊息
        """
        if update_date is None:
            update_date = datetime.now()
            
        self.db_manager.connect()
        
        try:
            # 獲取需要更新的股票清單
            followed_stocks = self.db_manager.get_followed_stocks()
            updated_stocks = []
            failed_stocks = []
            
            for stock_id, stock_name in followed_stocks:
                try:
                    # 獲取足夠計算 KD 值的歷史資料 (至少需要 period + 1 天)
                    # 多取一些資料以確保有足夠的資料計算
                    days_needed = self.period + 20
                    
                    # 修改查詢，確保取得的資料時間不超過 update_date
                    query = f"""
                        SELECT date, stock_id, highest_price, lowest_price, closing_price
                        FROM stock_daily
                        WHERE stock_id = ? AND date <= ?
                        ORDER BY date DESC
                        LIMIT {days_needed}
                    """
                    
                    result = self.db_manager.conn.execute(query, [stock_id, update_date]).fetchall()
                    
                    if len(result) < self.period:
                        logger.warning(f"Not enough data to calculate KD for {stock_id} {stock_name}")
                        failed_stocks.append((stock_id, stock_name))
                        continue
                    
                    # 將結果轉換為 DataFrame 並按日期排序
                    df = pd.DataFrame(result, columns=['date', 'stock_id', 'highest_price', 'lowest_price', 'closing_price'])
                    df = df.sort_values('date')
                    
                    # 計算 KD 值
                    df = self.calculate_kd(df)
                    
                    # 獲取最新日期的 KD 值
                    latest_data = df.iloc[-1]
                    latest_date = latest_data['date']
                    k_value = latest_data['k_value']
                    d_value = latest_data['d_value']
                    
                    # 更新資料庫
                    self.db_manager.conn.execute("""
                        UPDATE stock_daily 
                        SET k_value = ?, d_value = ?
                        WHERE stock_id = ? AND date = ?
                    """, [k_value, d_value, stock_id, latest_date])
                    
                    updated_stocks.append((stock_id, stock_name))
                    # logger.info(f"Successfully updated KD values for {stock_id} {stock_name}")
                    
                except Exception as e:
                    logger.exception(f"Error calculating KD for {stock_id}: {e}")
                    failed_stocks.append((stock_id, stock_name))
            
            self.db_manager.conn.commit()
            
            # 處理最終結果
            if not failed_stocks:
                success_message = (
                    f"KD calculation completed successfully for {update_date.strftime('%Y-%m-%d')}. "
                    f"Updated {len(updated_stocks)} stocks."
                )
                logger.info(success_message)
                return True, success_message
            else:
                failed_message = (
                    f"KD calculation completed with some failures for {update_date.strftime('%Y-%m-%d')}. "
                    f"Updated {len(updated_stocks)} stocks. "
                    f"Failed stocks: {', '.join([f'{id}({name})' for id, name in failed_stocks])}"
                )
                logger.warning(failed_message)
                return len(failed_stocks) < len(updated_stocks), failed_message
                
        except Exception as e:
            error_message = f"Error during KD calculation: {e}"
            logger.exception(error_message)
            return False, error_message
        
        finally:
            self.db_manager.close()
    
    def calculate_kd(self, df):
        """
        計算 KD 值
        Args:
            df: 包含 highest_price, lowest_price, closing_price 的 DataFrame
        Returns:
            添加了 k_value 和 d_value 列的 DataFrame
        """
        # 確保數據為數值型別
        numeric_columns = ['highest_price', 'lowest_price', 'closing_price']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 處理可能的 NaN 值，並明確創建一個副本
        df = df.dropna(subset=numeric_columns).copy()
        
        # 計算 n 日內的最高價和最低價
        df['highest_n'] = df['highest_price'].rolling(window=self.period, min_periods=1).max()
        df['lowest_n'] = df['lowest_price'].rolling(window=self.period, min_periods=1).min()
        
        # 計算 RSV (Raw Stochastic Value)
        df['rsv'] = 100 * (df['closing_price'] - df['lowest_n']) / (df['highest_n'] - df['lowest_n'] + 1e-9)
        
        # 初始化 K 和 D 值
        df['k_value'] = 50.0
        df['d_value'] = 50.0
        
        # 計算 K 和 D 值 (使用遞迴公式)
        for i in range(1, len(df)):
            df.loc[df.index[i], 'k_value'] = (2/3) * df.loc[df.index[i-1], 'k_value'] + (1/3) * df.loc[df.index[i], 'rsv']
            df.loc[df.index[i], 'd_value'] = (2/3) * df.loc[df.index[i-1], 'd_value'] + (1/3) * df.loc[df.index[i], 'k_value']
        
        # 清理中間計算列
        df = df.drop(['highest_n', 'lowest_n', 'rsv'], axis=1)
        
        return df


if __name__ == "__main__":
    db_manager = DatabaseManager(
        db_path="StockHero.db",
        bucket_name="ian-line-bot-files"
    )
    
    calculator = KDCalculator(db_manager)
    test_date = datetime(2025, 3, 25)
    success, message = calculator.calculate_kd_values(test_date)
    print(message)