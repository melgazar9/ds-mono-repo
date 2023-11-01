from routes import *
from waitress import serve
from apscheduler.schedulers.background import BackgroundScheduler
from configparser import ConfigParser
import logging


ENVIRONMENT = os.getenv('ENVIRONMENT').split(',')

### Run app ###

config = ConfigParser()
config.read('config.ini')

scheduler = BackgroundScheduler()

if ENVIRONMENT == 'dev':
    print('\n*** Running environment dev. ***\n')
    cron = json_string_to_dict(config['TAP_YFINANCE']['DEV_CRON_PARAMS'])
    scheduler.add_job(tap_yfinance_dev, trigger='cron', **cron)

if ENVIRONMENT == 'production':
    print('\n*** Running environment production. ***\n')
    cron = json_string_to_dict(config['TAP_YFINANCE']['PRODUCTION_CRON_PARAMS'])
    scheduler.add_job(tap_yfinance_production, trigger='cron', **cron)


if __name__ == "__main__":
    HOST = '0.0.0.0'
    PORT = 5000
    logging.root.setLevel(logging.INFO)
    logging.warning(f'Server is listening on port {PORT}')
    logging.warning(f'Hosting environment {ENVIRONMENT}')

    scheduler.start()
    # app.run(host=HOST, port=PORT, debug=True)  # debug mode
    serve(app, host=HOST, port=PORT, threads=2)  # waitress wsgi production server
