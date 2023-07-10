from flask import Flask, make_response, request, redirect, Blueprint, render_template
from ds_core.ds_utils import *


app = Flask(__name__)
app.url_map.strict_slashes = False

@app.route('/')
def index():
    return 'Financial-ETL hosting successful.'

@app.route('/healthcheck', methods=['GET'])
def healthcheck():
    with app.app_context():
        return make_response("Healthcheck successful.", 200)

@app.route('/financial-etl', methods=['GET'])
def financial_etl():
    with app.app_context():
        return make_response("Financial-ETL is running.", 200)

@app.route('/financial-etl/yfinance', methods=['GET'])
def yfinance_etl():
    with app.app_context():
        return make_response("Financial-ETL yfinance is running.", 200)


### run ETL routes ###

@app.route('/financial-etl/yfinance/prices', methods=['GET'])
def yfinance_etl_prices():
    with app.app_context():
        project_dir = 'yfinance/prices'
        run_command = 'python run_etl_prices.py'
        shell_command = f'cd {os.path.join(app.root_path)}/{project_dir}; {run_command};'
        subprocess.run(shell_command, shell=True)
        return make_response(f'Last ran project {project_dir} at {cur_timestamp(clean_string=False)}.', 200)
