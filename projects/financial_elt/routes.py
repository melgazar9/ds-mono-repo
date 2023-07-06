from flask import Flask, make_response, request, redirect, Blueprint, render_template
from ds_core.ds_utils import *


app = Flask(__name__)
app.url_map.strict_slashes = False

@app.route('/')
def index():
    return 'Financial ELT is running :)'

@app.route('/healthcheck', methods=['GET'])
def healthcheck():
    with app.app_context():
        return make_response("Server is running...", 200)


@app.route('/financial-elt/yfinance/stock-prices', methods=['GET'])
def yfinance_etl_prices():
    with app.app_context():
        project_dir = 'yfinance/stock_prices'
        run_command = 'python run_etl_prices.py'
        shell_command = f'cd {os.path.join(app.root_path)}/{project_dir}; {run_command};'
        subprocess.run(shell_command, shell=True)
        return make_response(f'Last ran project {project_dir} at {cur_timestamp(clean_string=False)}.', 200)
