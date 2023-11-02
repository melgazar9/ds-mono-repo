from flask import Flask, make_response
import os
import subprocess
from datetime import datetime
from configparser import ConfigParser

app = Flask(__name__)
app.url_map.strict_slashes = False

config = ConfigParser()
config.read('config.ini')

TAP_YFINANCE_TARGET = config['TAP_YFINANCE']['TAP_YFINANCE_TARGET']

def cur_timestamp():
    return datetime.today().replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')

@app.route('/')
def index():
    return 'tap-yfinance is running...'


###### healthcheck ######

@app.route('/financial-elt/yfinance/healthcheck', methods=['GET'])
def healthcheck():
    with app.app_context():
        return make_response("tap-yfinance healthcheck successful...", 200)

###### tap yfinance routes ######

@app.route('/financial-elt/yfinance/tap-yfinance-dev', methods=['GET'])
def tap_yfinance_dev():
    with app.app_context():
        run_command = f'meltano --environment=dev el tap-yfinance target-{TAP_YFINANCE_TARGET} --state-id tap_yfinance_dev'
        shell_command = f'cd {os.path.join(app.root_path)}; {run_command};'
        subprocess.run(shell_command, shell=True)
        return make_response(f'Last ran project tap-yfinance-dev at {cur_timestamp()}.', 200)

@app.route('/financial-elt/yfinance/tap-yfinance-production', methods=['GET'])
def tap_yfinance_production():
    with app.app_context():
        run_command = f'meltano --environment=production el tap-yfinance target-{TAP_YFINANCE_TARGET} --state-id tap_yfinance_production'
        shell_command = f'cd {os.path.join(app.root_path)}; {run_command};'
        subprocess.run(shell_command, shell=True)
        return make_response(f'Last ran project tap-yfinance-production at {cur_timestamp()}.', 200)