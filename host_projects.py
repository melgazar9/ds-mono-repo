from flask import Flask, make_response, request, render_template
from waitress import serve
from apscheduler.schedulers.background import BackgroundScheduler
import yaml
from ds_core.ds_utils import *


""" Script to host projects specified in config.yaml """


PROJECTS_TO_HOST = yaml.safe_load(open('config.yaml', 'r'))
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

app = Flask(__name__)
scheduler = BackgroundScheduler()

@app.route('/healthcheck', methods=['GET'])
def healthcheck():
    with app.app_context():
        return make_response("Server is running...", 200)

if PROJECTS_TO_HOST['numerai']['numerai_signals']:
    @app.route('/numerai-signals', methods=['GET'])
    def numerai_signals():
        with app.app_context():
            project_dir = 'numerai/numerai_signals/'
            run_command = 'python run_numerai_signals.py'
            shell_command = f'cd {os.path.join(app.root_path)}/{project_dir}; {run_command};'
            subprocess.run(shell_command, shell=True)
            return make_response(f'Last ran project {project_dir} at {cur_timestamp(clean_string=False)}.', 200)

    cron = json_string_to_dict(PROJECTS_TO_HOST['numerai']['numerai_signals']['numerai_signals_cron'])
    scheduler.add_job(numerai_signals, trigger='cron', **cron)


if __name__ == "__main__":
    PORT = 5000
    HOST = '0.0.0.0'
    logging.root.setLevel(logging.DEBUG)
    logging.info(f'Server is listening on port {PORT}')
    logging.info(f'Hosting projects {PROJECTS_TO_HOST}')
    scheduler.start()

    # app.run(host=HOST, port=PORT, debug=True)  # debug mode
    serve(app, host=HOST, port=PORT)  # production