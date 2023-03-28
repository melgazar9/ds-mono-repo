from apscheduler.schedulers.background import BackgroundScheduler
import yaml
from routes import *

PROJECTS_TO_HOST = yaml.safe_load(open('config.yml', 'r'))

d = {}
for p in PROJECTS_TO_HOST:
    key = list(p.keys())[0]
    sub_dict = {}
    for v in p.values():
        for v2 in v:
            for v3 in v2.keys():
                sub_dict[v3] = v2[v3]
    d[key] = sub_dict

PROJECTS_TO_HOST = d.copy()
del d


### Run app ###

scheduler = BackgroundScheduler()

if PROJECTS_TO_HOST['financial_elt']['yfinance_elt']:
    cron = json_string_to_dict(PROJECTS_TO_HOST['financial_elt']['yfinance_elt_cron'])
    scheduler.add_job(yfinance_elt, trigger='cron', **cron)


if __name__ == "__main__":
    HOST = '0.0.0.0'
    PORT = 5000
    logging.root.setLevel(logging.DEBUG)
    logging.info(f'Server is listening on port {PORT}')
    logging.info(f'Hosting projects {PROJECTS_TO_HOST}')

    scheduler.start()

    # gunicorn wsgi production server

    workers = mp.cpu_count() * 2 + 1  # set the number of workers
    bind_address = f'{HOST}:{PORT}'
    shell_command = f"gunicorn -w {workers} -b {bind_address} {sys.argv[0].strip('.py')}:app"
    print(f"Starting server with command: {shell_command}")
    subprocess.run(shell_command, shell=True)

    # from gunicorn.app.wsgiapp import run
    # run()

    # app.run(host=HOST, port=PORT, debug=True)  # debug mode
    # serve(app, host=HOST, port=PORT)  # waitress production wsgi server
