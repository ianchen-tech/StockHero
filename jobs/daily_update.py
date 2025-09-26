import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import smtplib
from email.mime.text import MIMEText
from data.database.db_manager import DatabaseManager
from data.crawler.stock_update import StockUpdater
from data.crawler.ratio_update import RatioUpdater
from data.analysis.screening import StockScreener
from data.analysis.kd_calculator import KDCalculator
from datetime import datetime
from config.logger import setup_logging

# 設置 logger
logger = setup_logging()

def send_email(subject, body, to_email, sender_email, sender_password):
    """
    使用 Gmail SMTP 發送郵件。
    """
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = to_email

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
            smtp_server.login(sender_email, sender_password)
            smtp_server.sendmail(sender_email, to_email, msg.as_string())
        logger.info(f"Email sent successfully to {to_email}")
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")

def update_stock_data(update_date: datetime = None):
    """
    執行股票資料更新
    Args:
        update_date: 指定要更新的日期，如果不指定則使用當天
    """
    status_message = ""
    success_status = False
    has_warnings = False  # 新增：追蹤是否有警告
    email_subject = ""
    email_body = ""
    
    # 從環境變數讀取郵件設定
    sender_email = "ianchentech@gmail.com"
    sender_password = os.getenv("GMAIL_SENDER_PASSWORD")
    receiver_email = "ian@cloud-latitude.com"

    if not sender_password:
        logger.warning("Email configuration (GMAIL_SENDER_PASSWORD) is not set. Email notification will be skipped.")
        send_email_notification = False
    else:
        send_email_notification = True

    try:
        # 如果沒有指定日期，使用當天
        if update_date is None:
            update_date = datetime.now()

        logger.info(f"Starting stock data update for date: {update_date.strftime('%Y-%m-%d')}")
        
        # 初始化資料庫管理器
        db_manager = DatabaseManager(
            db_path="StockHero.db",
            bucket_name="ian-line-bot-files"
        )
        
        # 執行更新
        updater = StockUpdater(db_manager)
        update_success, update_message = updater.update_daily_data(update_date)
        
        # 記錄股票更新狀態
        if not update_success:
            logger.warning(f"Stock data update completed with warnings: {update_message}")
            has_warnings = True
        else:
            logger.info(f"Stock data update completed successfully: {update_message}")
        
        # 無論股票更新是否完全成功，都繼續執行後續任務
        # 執行本益比、淨值比和殖利率更新
        ratio_updater = RatioUpdater(db_manager)
        ratio_success, ratio_message = ratio_updater.update_ratio_data(update_date)
        
        if not ratio_success:
            logger.warning(f"Ratio update warning: {ratio_message}")
            has_warnings = True
        else:
            logger.info(f"Ratio update completed successfully: {ratio_message}")
        
        # 執行 KD 值計算
        kd_calculator = KDCalculator(db_manager)
        kd_success, kd_message = kd_calculator.calculate_kd_values(update_date)
        
        if not kd_success:
            logger.warning(f"KD calculation warning: {kd_message}")
            has_warnings = True
        else:
            logger.info(f"KD calculation completed successfully: {kd_message}")
        
        # 執行條件篩選
        screener = StockScreener(db_manager)
        screen_success, screen_message = screener.screen_stocks(update_date)
        
        if screen_success:
            logger.info(f"Stock screening completed successfully: {screen_message}")
        else:
            logger.error(f"Stock screening failed: {screen_message}")
            has_warnings = True
        
        # 根據整體執行結果設定狀態
        if has_warnings:
            success_status = True  # 整體仍視為成功，但有警告
            warning_details = []
            if not update_success:
                warning_details.append(f"股票更新: {update_message}")
            if not ratio_success:
                warning_details.append(f"比率更新: {ratio_message}")
            if not kd_success:
                warning_details.append(f"KD計算: {kd_message}")
            if not screen_success:
                warning_details.append(f"條件篩選: {screen_message}")
            
            status_message = f"[{update_date.strftime('%Y-%m-%d')}] Update completed with warnings: {'; '.join(warning_details)}"
        else:
            success_status = True
            status_message = f"[{update_date.strftime('%Y-%m-%d')}] All tasks completed successfully."
            
    except Exception as e:
        error_message = f"[{update_date.strftime('%Y-%m-%d') if update_date else 'Unknown Date'}] Update failed: {str(e)}"
        logger.exception("Unexpected error during update")
        success_status = False
        status_message = error_message
    finally:
        if send_email_notification:
            if success_status and not has_warnings:
                email_subject = f"StockHero Daily Update Successful for {update_date.strftime('%Y-%m-%d') if update_date else 'Unknown Date'}"
                status_text = "Success"
            elif success_status and has_warnings:
                email_subject = f"StockHero Daily Update WARNING for {update_date.strftime('%Y-%m-%d') if update_date else 'Unknown Date'}"
                status_text = "Success with Warnings"
            else:
                email_subject = f"StockHero Daily Update FAILED for {update_date.strftime('%Y-%m-%d') if update_date else 'Unknown Date'}"
                status_text = "Failure"
            
            email_body = f'''Update process finished.

Status: {status_text}
Message: {status_message}

Detailed logs are available in the system.'''
            send_email(email_subject, email_body, receiver_email, sender_email, sender_password)
        
        # 返回原始的成功狀態和訊息
        if success_status : # 這裡的條件是為了確保能回傳原始的 True/False 給呼叫者
             return True, status_message.split("] ",1)[1] if "] " in status_message else status_message
        else:
             return False, status_message.split("] ",1)[1] if "] " in status_message else status_message


if __name__ == "__main__":
    update_date = datetime.strptime("2025-09-25", "%Y-%m-%d")
    # update_date = None # 改為 None 以使用當前日期
    success, message = update_stock_data(update_date)
    print(f"Execution finished. Success: {success}, Message: {message}")