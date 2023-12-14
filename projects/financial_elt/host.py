from routes import *
from waitress import serve
from apscheduler.schedulers.background import BackgroundScheduler

import logging
import json

ENVIRONMENT = os.getenv('ENVIRONMENT')

tap_yfinance_functions = {
    'dev': tap_yfinance_dev,
    'production': tap_yfinance_production
}

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f'\n*** Running environment {ENVIRONMENT}. ***\n')

    scheduler = BackgroundScheduler(job_defaults={'max_instances': 2})

    ###### tap-yfinance ######

    tap_yfinance_cron = json.loads(os.getenv('TAP_YFINANCE_CRON'))
    scheduler.add_job(tap_yfinance_functions[ENVIRONMENT], trigger='cron', **tap_yfinance_cron, jitter=120)

    ###### host ######

    HOST = '0.0.0.0'
    PORT = 5000
    logging.info(f'Server is listening on port {PORT}')
    logging.info(f'Hosting environment {ENVIRONMENT}')

    scheduler.start()

    serve(app, host=HOST, port=PORT, threads=2)  # waitress wsgi production server
