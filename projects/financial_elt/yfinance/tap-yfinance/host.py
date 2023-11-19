from routes import *
from waitress import serve
from apscheduler.schedulers.background import BackgroundScheduler
from configparser import ConfigParser
import logging
import json

ENVIRONMENT = os.getenv('ENVIRONMENT')

### Run app ###

config = ConfigParser()
config.read('config.ini')

scheduler = BackgroundScheduler(job_defaults={'max_instances': 2})

if ENVIRONMENT == 'dev':
    print('\n*** Running environment dev. ***\n')
    cron = json.loads(config['TAP_YFINANCE']['DEV_CRON_PARAMS'])
    scheduler.add_job(tap_yfinance_dev, trigger='cron', **cron)

if ENVIRONMENT == 'production':
    print('\n*** Running environment production. ***\n')
    cron = json.loads(config['TAP_YFINANCE']['PRODUCTION_CRON_PARAMS'])
    scheduler.add_job(tap_yfinance_production, trigger='cron', **cron)


if __name__ == "__main__":
    HOST = '0.0.0.0'
    PORT = 5000
    logging.root.setLevel(logging.INFO)
    logging.warning(f'Server is listening on port {PORT}')
    logging.warning(f'Hosting environment {ENVIRONMENT}')

    scheduler.start()
    serve(app, host=HOST, port=PORT, threads=2)  # waitress wsgi production server