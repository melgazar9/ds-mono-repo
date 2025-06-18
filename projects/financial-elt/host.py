import json
import logging
import signal

from apscheduler.schedulers.background import BackgroundScheduler
from routes import *
from utils import *
from waitress import serve

ENVIRONMENT = os.getenv("ENVIRONMENT")

signal.signal(signal.SIGINT, critical_shutdown_handler)
signal.signal(signal.SIGTERM, critical_shutdown_handler)


if __name__ == "__main__":
    # Note: Setting mp.set_start_method("spawn") tells multiprocessing to create new Python interpreter processes from
    # scratch, rather than duplicating the current process's memory. This prevents child processes from inheriting the
    # Flask application's state, open file descriptors, or locks, which are common causes of deadlocks.

    mp.set_start_method("spawn", force=True)
    setup_logging()

    scheduler = BackgroundScheduler(job_defaults={"max_instances": 3})

    ###### tap-yfinance ######

    if "tap-yfinance" in os.getenv("FINANCIAL_ELT_TAPS_TO_RUN"):
        tap_yfinance_cron = json.loads(os.getenv("TAP_YFINANCE_CRON"))
        scheduler.add_job(tap_yfinance, trigger="cron", **tap_yfinance_cron, jitter=120)
        logging.info(f"Added tap-yfinance job with cron: {tap_yfinance_cron}")

    if "tap-yahooquery" in os.getenv("FINANCIAL_ELT_TAPS_TO_RUN"):
        tap_yahooquery_cron = json.loads(os.getenv("TAP_yahooquery_CRON"))
        scheduler.add_job(
            tap_yahooquery, trigger="cron", **tap_yahooquery_cron, jitter=120
        )
        logging.info(f"Added tap-yahooquery job with cron: {tap_yahooquery_cron}")

    if "tap-polygon" in os.getenv("FINANCIAL_ELT_TAPS_TO_RUN"):
        tap_polygon_cron = json.loads(os.getenv("TAP_POLYGON_CRON"))
        scheduler.add_job(tap_polygon, trigger="cron", **tap_polygon_cron, jitter=120)
        logging.info(f"Added tap-polygon job with cron: {tap_polygon_cron}")

    ###### host ######

    HOST = "0.0.0.0"
    PORT = 5000
    logging.info(f"Server is listening on port {PORT}")
    logging.info(f"Hosting environment {ENVIRONMENT}")

    scheduler.start()

    if DEBUG:
        tap = MeltanoTap(
            project_dir="tap-polygon",
            num_workers=10,
            tap_name="tap-polygon",
            target_name="jsonl",
        )
        tap.run_tap()

    serve(app, host=HOST, port=PORT, threads=2)  # waitress wsgi production server
