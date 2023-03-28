from flask import Flask, make_response, request, redirect, Blueprint, render_template
from ds_core.ds_utils import *


app = Flask(__name__)
app.url_map.strict_slashes = False

@app.route('/')
def index():
    return 'Hello Flask App :)'

@app.route('/healthcheck', methods=['GET'])
def healthcheck():
    with app.app_context():
        return make_response("Server is running...", 200)


@app.route('/financial-elt/yfinance-elt', methods=['GET'])
def yfinance_elt():
    with app.app_context():
        project_dir = 'yfinance/'
        run_command = 'python populate_database.py'
        shell_command = f'cd {os.path.join(app.root_path)}/{project_dir}; {run_command};'
        subprocess.run(shell_command, shell=True)
        return make_response(f'Last ran project {project_dir} at {cur_timestamp(clean_string=False)}.', 200)
