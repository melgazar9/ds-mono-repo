from flask import Flask, make_response, request, redirect, Blueprint, render_template
import os
import subprocess

from datetime import datetime

app = Flask(__name__)
app.url_map.strict_slashes = False


### GLOBALS ###

ENVIRONMENT = os.getenv('ENVIRONMENT')

TAP_YFINANCE_TARGET = os.getenv('TAP_YFINANCE_TARGET')

assert isinstance(TAP_YFINANCE_TARGET, str), 'could not determine yfinance target'


def cur_timestamp(utc=True):
    if utc:
        return datetime.utcnow().replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
    else:
        return datetime.now().replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')

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

###### tap yfinance routes ######

@app.route(f'/financial-elt/yfinance/tap-yfinance-{ENVIRONMENT}', methods=['GET'])
def tap_yfinance():
    with app.app_context():
        project_dir = 'yfinance/tap-yfinance'
        run_command = f'meltano --environment={ENVIRONMENT} el tap-yfinance target-{TAP_YFINANCE_TARGET} --state-id tap_yfinance_{ENVIRONMENT}_{TAP_YFINANCE_TARGET}'
        subprocess.Popen(run_command, shell=True, cwd=os.path.join(app.root_path, project_dir))
        return make_response(f'Last ran project tap-yfinance-{ENVIRONMENT} target {TAP_YFINANCE_TARGET} at {cur_timestamp()}.', 200)
