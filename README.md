# StockHero

---
Folder structure：

```
StockHero/
├── api/                        # API 端點
│   └── main.py                 # API 主程式
├── data/                       # 資料處理相關
│   ├── crawler/                # 資料抓取模組
│   │   ├── stocks_info.py      # 各產業股票列表
│   │   ├── stock_history.py    # 股票歷史資料
│   │   └── stock_update.py     # 股票資料更新
│   ├── database/               # 資料庫相關
│   │   ├── db_manager.py       # 資料庫管理
│   │   └── models.py           # 資料模型定義
│   └── analysis/               # 分析邏輯
│       ├── technical.py        # 技術分析
│       └── screening.py        # 選股邏輯
├── jobs/                       # 排程任務
│   └── daily_update.py         # 每日更新資料
├── config/                     # 設定檔
│   ├── config.py               # 一般設定
│   └── logger.py               # logging 設置
├── utils/                      # 通用工具函數目錄
├── tests/                      # 測試檔案
├── notebook/                   # 筆記
├── Dockerfile                  # Docker 設定
└── README.md                   # 專案說明
```