from routes import *
from waitress import serve
from apscheduler.schedulers.background import BackgroundScheduler
from configparser import ConfigParser
import logging
import json

ENVIRONMENT = os.getenv('ENVIRONMENT')

tap_yfinance_functions = {
    'dev': tap_yfinance_dev,
    'production': tap_yfinance_production
}

config = ConfigParser()

config.read('config.ini')

# if config['TAP_YFINANCE']['VM'] == 'gcp':
#     import google.cloud.logging
#     client = google.cloud.logging.Client()
#     client.setup_logging()


if __name__ == "__main__":
    logging.info(f'\n*** Running environment {ENVIRONMENT}. ***\n')

    scheduler = BackgroundScheduler(job_defaults={'max_instances': 2})

    ###### tap-yfinance ######

    tap_yfinance_cron = json.loads(config['TAP_YFINANCE'][f'{ENVIRONMENT}_CRON_PARAMS'])
    scheduler.add_job(tap_yfinance_functions[ENVIRONMENT], trigger='cron', **tap_yfinance_cron, jitter=120)

    ###### host ######

    HOST = '0.0.0.0'
    PORT = 5000
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f'Server is listening on port {PORT}')
    logging.info(f'Hosting environment {ENVIRONMENT}')

    scheduler.start()

    # serve(app, host=HOST, port=PORT, threads=2)  # waitress wsgi production server
    app.run(debug=True)