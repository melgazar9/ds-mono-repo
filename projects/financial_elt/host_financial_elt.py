from apscheduler.schedulers.background import BackgroundScheduler
from waitress import serve
from routes import *

config = ConfigParser()
config.read('config.ini')


### Run app ###

scheduler = BackgroundScheduler()

if ast.literal_eval(config['YF_STOCK_PRICE_ETL']['host_yf_stock_price_etl']):
    cron = json_string_to_dict(config['YF_STOCK_PRICE_ETL']['yfinance_stock_price_etl_cron'])
    scheduler.add_job(yfinance_etl_prices, trigger='cron', **cron)


if __name__ == "__main__":
    HOST = '0.0.0.0'
    PORT = 5000
    logging.root.setLevel(logging.DEBUG)
    logging.info(f'Server is listening on port {PORT}')
    logging.info(f'Hosting projects {config}')

    scheduler.start()

    # gunicorn wsgi production server

    # workers = mp.cpu_count() * 2 + 1  # set the number of workers
    # bind_address = f'{HOST}:{PORT}'
    # shell_command = f"gunicorn -w {workers} -b {bind_address} {sys.argv[0].strip('.py')}:app"
    # print(f"Starting server with command: {shell_command}")
    # subprocess.run(shell_command, shell=True)

    # from gunicorn.app.wsgiapp import run
    # run()

    # app.run(host=HOST, port=PORT, debug=True)  # debug mode
    serve(app, host=HOST, port=PORT, threads=8)  # waitress production wsgi server
