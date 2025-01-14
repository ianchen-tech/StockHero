import sys, os
import asyncio
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import FastAPI, HTTPException, BackgroundTasks
from jobs.daily_update import update_stock_data
from config.logger import setup_logging

app = FastAPI()
logger = setup_logging()

async def run_update_in_background(update_date):
    try:
        success, message = update_stock_data(update_date)
        logger.info(f"Background task completed: {message}")
    except Exception as e:
        logger.exception("Background task failed")

@app.get("/update-stock")
async def trigger_stock_update(background_tasks: BackgroundTasks, date: str = None):
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
        background_tasks.add_task(run_update_in_background, update_date)
        return {"status": "accepted", "message": "Update process started in background"}
    except Exception as e:
        logger.exception("API endpoint error")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/activate")
async def activate_service():
    """
    激活服務的API端點
    """
    try:
        logger.info("Service activation request received")
        return {"status": "success", "message": "Service activated"}
    except Exception as e:
        logger.exception("Service activation failed")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)