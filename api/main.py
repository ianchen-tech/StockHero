import sys, os
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import FastAPI, HTTPException
from jobs.daily_update import update_stock_data
from config.logger import setup_logging

app = FastAPI()
logger = setup_logging()

@app.get("/update-stock")
async def trigger_stock_update(date: str = None):
    try:
        if date:
            try:
                update_date = datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(
                    status_code=400, 
                    detail="Invalid date format. Please use YYYY-MM-DD format"
                )
        else:
            update_date = None
            
        success, message = update_stock_data(update_date)
        return {"status": "done", "message": message}
    except Exception as e:
        logger.exception("API endpoint error")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)