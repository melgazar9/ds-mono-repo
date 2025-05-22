import json
import signal

from apscheduler.schedulers.background import BackgroundScheduler
from routes import *
from utils import *
from waitress import serve

ENVIRONMENT = os.getenv("ENVIRONMENT")

signal.signal(signal.SIGINT, critical_shutdown_handler)
signal.signal(signal.SIGTERM, critical_shutdown_handler)


if __name__ == "__main__":
    setup_logging()

    scheduler = BackgroundScheduler(job_defaults={"max_instances": 3})

    ###### tap-yfinance ######

    # tap_yfinance_cron = json.loads(os.getenv("TAP_YFINANCE_CRON"))
    # scheduler.add_job(tap_yfinance, trigger="cron", **tap_yfinance_cron, jitter=120)

    tap_polygon_cron = json.loads(os.getenv("TAP_POLYGON_CRON"))
    scheduler.add_job(tap_polygon, trigger="cron", **tap_polygon_cron, jitter=120)

    ###### host ######

    HOST = "0.0.0.0"
    PORT = 8888
    logging.info(f"Server is listening on port {PORT}")
    logging.info(f"Hosting environment {ENVIRONMENT}")

    scheduler.start()

    serve(app, host=HOST, port=PORT, threads=2)  # waitress wsgi production server
