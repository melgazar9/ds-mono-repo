from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from waitress import serve
import json

import logging
from routes import *


ENVIRONMENT = os.getenv('ENVIRONMENT')
assert ENVIRONMENT is not None, 'Could not parse ENVIRONMENT.'

config = ConfigParser()
config.read('config.ini')

### Run app ###

scheduler = BackgroundScheduler(job_defaults={'max_instances': 2})

projects_to_host = []

if config['YFINANCE']['projects_to_host'].split(','):
    projects_to_host.append(config['YFINANCE']['projects_to_host'])
    cron = json.loads(config['YFINANCE'][f'tap_yfinance_cron_{ENVIRONMENT.lower()}'])
    job_name = f'tap_yfinance_{ENVIRONMENT.lower()}'
    scheduler.add_job(locals().get(job_name), trigger='cron', **cron)

if __name__ == "__main__":
    HOST = '0.0.0.0'
    PORT = 5000
    logging.warning(f'Server is listening on port {PORT}')
    logging.warning(f'Hosting Financial-ELT Projects: {projects_to_host}')

    scheduler.start()

    app.run(host=HOST, port=PORT, debug=True)  # debug mode
    # serve(app, host=HOST, port=PORT, threads=2)  # waitress production wsgi server