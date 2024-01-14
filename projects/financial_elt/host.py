from routes import *
from waitress import serve
from apscheduler.schedulers.background import BackgroundScheduler

import logging
import json

ENVIRONMENT = os.getenv('ENVIRONMENT')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f'\n*** Running environment {ENVIRONMENT}. ***\n')

    scheduler = BackgroundScheduler(job_defaults={'max_instances': 3})

    ###### tap-yfinance ######

    num_tasks = int(os.getenv('TAP_YFINANCE_NUM_WORKERS'))
    tap_yfinance_cron = json.loads(os.getenv('TAP_YFINANCE_CRON'))

    if num_tasks == 1:
        scheduler.add_job(tap_yfinance, trigger='cron', **tap_yfinance_cron, jitter=120)
    else:
        import yaml

        assert isinstance(num_tasks, int) and num_tasks > 1, \
            f"ENV variable TAP_YFINANCE_NUM_WORKERS must be >= 1. It is currently set to {num_tasks} with datatype {type(num_tasks)}"

        with open("yfinance/tap-yfinance/meltano.yml", "r") as meltano_cfg:
            cfg = yaml.safe_load(meltano_cfg)

        tasks = cfg.get('plugins').get('extractors')[0].get('select')
        tasks = [f'--select {i}' for i in tasks]
        tasks_per_chunk = len(tasks) // num_tasks
        remainder = len(tasks) % num_tasks

        task_chunks = []
        start_index = 0
        for i in range(num_tasks):
            chunk_size = tasks_per_chunk + (1 if i < remainder else 0)
            task_chunks.append(tasks[start_index: start_index + chunk_size])
            start_index += chunk_size
        
        scheduler.add_job(tap_yfinance, kwargs={'task_chunks': task_chunks}, trigger='cron', **tap_yfinance_cron, jitter=120)


    ###### host ######

    HOST = '0.0.0.0'
    PORT = 5000
    logging.info(f'Server is listening on port {PORT}')
    logging.info(f'Hosting environment {ENVIRONMENT}')

    scheduler.start()

    serve(app, host=HOST, port=PORT, threads=2)  # waitress wsgi production server
