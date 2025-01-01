# StockHero

---
Folder structure：

```
StockHero/
├── app/						# Streamlit 主目錄
│   ├── pages/					# 多頁面目錄
│   │   ├── stock_detail.py		# 個股資訊與技術圖
│   │   ├── stock_screener.py	# 個股推薦
│   │   └── institutional.py	# 法人買賣趨勢
│   ├── components/				# 可重用的 UI 組件
│   │   ├── charts.py			# 圖表相關組件
│   │   └── widgets.py			# 其他 UI 組件
│   └── main.py					# Streamlit 主程式
├── data/						# 資料處理相關
│   ├── crawler/				# 資料抓取模組
│   │   └── stock_crawler.py	# 股票資料爬蟲
│   ├── database/				# 資料庫相關
│   │   ├── db_manager.py		# 資料庫管理
│   │   └── models.py			# 資料模型定義
│   └── analysis/				# 分析邏輯
│       ├── technical.py		# 技術分析
│       └── screening.py		# 選股邏輯
├── jobs/						# 排程任務
│   └── daily_update.py			# 每日更新資料
├── config/						# 設定檔
│   └── config.py				# 一般設定
├── utils/						# 通用工具函數目錄
├── tests/						# 測試檔案
├── notebook/                   # 筆記
├── requirements.txt			# 依賴套件
├── Dockerfile					# Docker 設定
└── README.md					# 專案說明
```