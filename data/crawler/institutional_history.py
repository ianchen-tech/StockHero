import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from data.database.db_manager import DatabaseManager
from data.database.models import StockDB

class InstitutionalHistoryCrawler:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.base_url = "https://www.twse.com.tw/rwd/zh/fund/T86"
        self.max_retries = 20
        self.retry_delay = 5
        # 定義產業類別和對應代號
        self.industry_codes = {
            # 電子科技相關
            '24': '半導體業',
            '28': '電子零組件業',
            '25': '電腦及週邊設備業',
            '26': '光電業',
            '27': '通信網路業',
            '29': '電子通路業',
            '30': '資訊服務業',
            '13': '電子工業',
            '31': '其他電子業',
            # 生技醫療相關
            '22': '生技醫療業',
            # 傳統產業
            '01': '水泥工業',
            '02': '食品工業',
            '03': '塑膠工業',
            '05': '電機機械',
            '06': '電器電纜',
            '07': '化學生技醫療',
            '21': '化學工業',
            '11': '橡膠工業',
            '08': '玻璃陶瓷',
            '09': '造紙工業',
            '10': '鋼鐵工業',
            '12': '汽車工業',
            '14': '建材營造',
            '23': '油電燃氣業',
            # 其他產業
            '15': '航運業',
            '18': '貿易百貨',
            '37': '運動休閒',
            '38': '居家生活',
            '35': '綠能環保',
            '36': '數位雲端',
            '20': '其他業'
        }

    def clean_number(self, value: str) -> float:
        """清理數字字串，移除逗號並轉換為浮點數"""
        if not value or value.strip() == "--":
            return None
        return float(value.replace(",", ""))

    def fetch_institutional_history(self, date: datetime, industry_code: str):
        """從證交所抓取某日期特定產業的三大法人交易資料"""
        params = {
            "date": date.strftime("%Y%m%d"),
            "selectType": industry_code,
            "response": "json"
        }
        
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                if retry_count > 0:
                    retry_delay = self.retry_delay * (2 ** (retry_count - 1))
                    print(f"Retrying for date {date.strftime('%Y-%m-%d')} industry {self.industry_codes[industry_code]} (Attempt {retry_count + 1}/{self.max_retries})")
                    print(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)

                response = requests.get(self.base_url, params=params)
                data = response.json()
                
                # 檢查回應狀態
                if data.get("stat") == "很抱歉，沒有符合條件的資料!":
                    print(f"No data available for date {date.strftime('%Y-%m-%d')} industry {self.industry_codes[industry_code]} (非交易日或無資料)")
                    return None
                
                if data.get("stat") != "OK":
                    print(f"Error fetching data for date {date} industry {self.industry_codes[industry_code]}: {data.get('stat')}")
                    retry_count += 1
                    continue
                
                records = []
                row_error = False
                for row in data.get("data", []):
                    try:
                        record = (
                            date,                                  # 日期
                            row[0].strip(),                       # 證券代號
                            row[1].strip(),                       # 證券名稱
                            self.clean_number(row[2]),            # 外陸資買進股數(不含外資自營商)
                            self.clean_number(row[3]),            # 外陸資賣出股數(不含外資自營商)
                            self.clean_number(row[4]),            # 外陸資買賣超股數(不含外資自營商)
                            self.clean_number(row[5]),            # 外資自營商買進股數
                            self.clean_number(row[6]),            # 外資自營商賣出股數
                            self.clean_number(row[7]),            # 外資自營商買賣超股數
                            self.clean_number(row[8]),            # 投信買進股數
                            self.clean_number(row[9]),            # 投信賣出股數
                            self.clean_number(row[10]),           # 投信買賣超股數
                            self.clean_number(row[11]),           # 自營商買賣超股數
                            self.clean_number(row[12]),           # 自營商買進股數(自行買賣)
                            self.clean_number(row[13]),           # 自營商賣出股數(自行買賣)
                            self.clean_number(row[14]),           # 自營商買賣超股數(自行買賣)
                            self.clean_number(row[15]),           # 自營商買進股數(避險)
                            self.clean_number(row[16]),           # 自營商賣出股數(避險)
                            self.clean_number(row[17]),           # 自營商買賣超股數(避險)
                            self.clean_number(row[18]),           # 三大法人買賣超股數
                        )
                        records.append(record)
                    except (ValueError, IndexError) as e:
                        print(f"Error processing row for {date.strftime('%Y-%m-%d')} industry {self.industry_codes[industry_code]}-{row[0].strip()}: {e}")
                        row_error = True
                        break
                
                if row_error:
                    # 如果有任何一筆資料處理失敗，重新嘗試整個產業的資料
                    retry_count += 1
                    continue
                        
                return records
                
            except Exception as e:
                print(f"Error fetching data for date {date} industry {self.industry_codes[industry_code]}: {e}")
                retry_count += 1
                if retry_count >= self.max_retries:
                    return False  # 表示爬取失敗
                
        return False  # 表示爬取失敗

    def crawl_institutional_history(self, start_date: datetime, end_date: datetime):
        """爬取指定日期範圍內所有產業的三大法人交易資料"""
        current_date = start_date
        while current_date <= end_date:
            # 先用任一產業代碼測試該日期是否有資料
            test_industry = list(self.industry_codes.keys())[0]
            result = self.fetch_institutional_history(current_date, test_industry)
            
            if result is None:
                # 如果沒有資料(非交易日)，直接跳到下一天
                print(f"No trading data for {current_date.strftime('%Y-%m-%d')} (非交易日)")
                current_date += timedelta(days=1)
                time.sleep(2)
                continue
            
            if result is False:
                print(f"Failed to fetch data for {current_date.strftime('%Y-%m-%d')} - {self.industry_codes[test_industry]} after maximum retries")
            elif result:
                print(f"Successfully fetched {len(result)} records for {current_date.strftime('%Y-%m-%d')} - {self.industry_codes[test_industry]}")
                self.db_manager.upsert_institutional_data(result)
            
            # 如果第一個產業有資料，才繼續抓取其他產業
            for industry_code in list(self.industry_codes.keys())[1:]:
                result = self.fetch_institutional_history(current_date, industry_code)
                
                if result is False:
                    print(f"Failed to fetch data for {current_date.strftime('%Y-%m-%d')} - {self.industry_codes[industry_code]} after maximum retries")
                elif result:
                    print(f"Successfully fetched {len(result)} records for {current_date.strftime('%Y-%m-%d')} - {self.industry_codes[industry_code]}")
                    self.db_manager.upsert_institutional_data(result)
                
                # 避免請求過於頻繁
                time.sleep(2)
            
            current_date += timedelta(days=1)

def main():
    db_manager = DatabaseManager(
        db_path="StockHero.db",
        bucket_name="ian-line-bot-files"
    )
    db_manager.connect()
    
    crawler = InstitutionalHistoryCrawler(db_manager)
    
    # 設定爬取的時間範圍
    start_date = datetime(2024, 7, 1)
    end_date = datetime(2025, 1, 22)
    
    try:
        crawler.crawl_institutional_history(start_date, end_date)
    finally:
        db_manager.close()

if __name__ == "__main__":
    main()