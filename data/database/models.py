
class StockDB:
    # 定義建立資料表的 SQL
    CREATE_STOCK_DAILY_TABLE = """
        CREATE TABLE IF NOT EXISTS stock_daily (
            date DATE,
            stock_id VARCHAR,
            stock_name VARCHAR,
            trade_volume BIGINT,         -- 成交股數
            trade_value BIGINT,          -- 成交金額
            opening_price DOUBLE,         -- 開盤價
            highest_price DOUBLE,         -- 最高價
            lowest_price DOUBLE,          -- 最低價
            closing_price DOUBLE,         -- 收盤價
            price_change DOUBLE,          -- 漲跌價差
            change_percent DOUBLE,        -- 漲跌百分比
            transaction_count INT,        -- 成交筆數
            ma5 DOUBLE,                  -- 5日均線
            ma10 DOUBLE,                 -- 10日均線
            ma20 DOUBLE,                 -- 20日均線
            ma60 DOUBLE,                 -- 60日均線
            PRIMARY KEY (date, stock_id)
        )
    """
    
    CREATE_STOCK_INFO_TABLE = """
        CREATE TABLE IF NOT EXISTS stock_info (
            stock_id VARCHAR PRIMARY KEY,
            stock_name VARCHAR,
            industry VARCHAR,
            follow BOOLEAN,
            market_type VARCHAR,
            source VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    """
    
    # 定義常用的 SQL 查詢語句
    UPSERT_STOCK_INFO = """
        INSERT OR REPLACE INTO stock_info
        (stock_id, stock_name, industry, follow, market_type, source, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    UPSERT_DAILY_DATA = """
        INSERT OR REPLACE INTO stock_daily 
        (date, stock_id, stock_name, trade_volume, trade_value, 
         opening_price, highest_price, lowest_price, closing_price, 
         price_change, change_percent, transaction_count,
         ma5, ma10, ma20, ma60)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    GET_FOLLOWED_STOCKS = """
        SELECT stock_id, stock_name 
        FROM stock_info 
        WHERE follow = TRUE
    """

    GET_STOCK_HISTORY_FOR_MA = """
        SELECT date, stock_id, closing_price
        FROM stock_daily
        WHERE stock_id = ?
        ORDER BY date
    """

    GET_RECENT_STOCK_DATA_FOR_MA = """
        SELECT date, stock_id, closing_price
        FROM stock_daily
        WHERE stock_id = ?
        ORDER BY date DESC
        LIMIT 60
    """

    UPDATE_MA_VALUES = """
        UPDATE stock_daily
        SET ma5 = ?, ma10 = ?, ma20 = ?, ma60 = ?
        WHERE stock_id = ? AND date = ?
    """