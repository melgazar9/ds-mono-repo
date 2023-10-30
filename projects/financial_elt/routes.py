from flask import (
    Flask, make_response, request, redirect, Blueprint, render_template
)

import os
import subprocess
from configparser import ConfigParser
from datetime import datetime

config = ConfigParser()
config.read('config.ini')
app = Flask(__name__)
app.url_map.strict_slashes = False

### GLOBALS ###

TAP_YFINANCE_TARGET = config['YFINANCE']['tap_yfinance_target']
assert isinstance(TAP_YFINANCE_TARGET, str), 'could not determine yfinance target'
def cur_timestamp():
    return datetime.today().replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')

### GENERAL ROUTES ###

@app.route('/')
def index():
    return 'Financial-ELT hosting successful.'

@app.route('/healthcheck', methods=['GET'])
def healthcheck():
    with app.app_context():
        return make_response("Healthcheck successful.", 200)

@app.route('/financial-elt', methods=['GET'])
def financial_elt():
    with app.app_context():
        return make_response("Financial-ELT is running.", 200)

@app.route('/financial-elt/yfinance', methods=['GET'])
def yfinance_elt():
    with app.app_context():
        return make_response("Financial-ELT yfinance is running.", 200)

########################
###### elt routes ######
########################

### tap-yfinance ###

@app.route('/financial-elt/yfinance/tap-yfinance-dev', methods=['GET'])
def tap_yfinance_dev():
    with app.app_context():
        project_dir = 'yfinance/tap-yfinance'
        run_command = f'meltano --environment=dev el tap-yfinance target-{TAP_YFINANCE_TARGET} --state-id {TAP_YFINANCE_TARGET}_dev'
        shell_command = f'cd {os.path.join(app.root_path)}/{project_dir}; {run_command};'
        subprocess.run(shell_command, shell=True)
        return make_response(f'Last ran project {project_dir} at {cur_timestamp()}.', 200)

@app.route('/financial-elt/yfinance/tap-yfinance-testing', methods=['GET'])
def tap_yfinance_testing():
    with app.app_context():
        project_dir = 'yfinance/tap-yfinance'
        run_command = f'meltano --environment=testing el tap-yfinance target-{TAP_YFINANCE_TARGET} --state-id {TAP_YFINANCE_TARGET}_testing'
        shell_command = f'cd {os.path.join(app.root_path)}/{project_dir}; {run_command};'
        subprocess.run(shell_command, shell=True)

        return make_response(f'Last ran project {project_dir} at {cur_timestamp()}.', 200)

@app.route('/financial-elt/yfinance/tap-yfinance-staging', methods=['GET'])
def tap_yfinance_staging():
    with app.app_context():
        project_dir = 'yfinance/tap-yfinance'
        run_command = f'meltano --environment=staging el tap-yfinance target-{TAP_YFINANCE_TARGET} --state-id {TAP_YFINANCE_TARGET}_staging'
        shell_command = f'cd {os.path.join(app.root_path)}/{project_dir}; {run_command};'
        subprocess.run(shell_command, shell=True)

        return make_response(f'Last ran project {project_dir} at {cur_timestamp()}.', 200)

@app.route('/financial-elt/yfinance/tap-yfinance-production', methods=['GET'])
def tap_yfinance_production():
    with app.app_context():
        project_dir = 'yfinance/tap-yfinance'
        run_command = f'meltano --environment=production el tap-yfinance target-{TAP_YFINANCE_TARGET} --state-id {TAP_YFINANCE_TARGET}_production'
        shell_command = f'cd {os.path.join(app.root_path)}/{project_dir}; {run_command};'
        subprocess.run(shell_command, shell=True)
        return make_response(f'Last ran project {project_dir} at {cur_timestamp()}.', 200)
