from flask import Flask, make_response, request, redirect, Blueprint, render_template
import os
import subprocess
from datetime import datetime
import logging
import time
import multiprocessing as mp

app = Flask(__name__)
app.url_map.strict_slashes = False

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


### GLOBALS ###

ENVIRONMENT = os.getenv("ENVIRONMENT")

TAP_YFINANCE_TARGET = os.getenv("TAP_YFINANCE_TARGET")

assert isinstance(TAP_YFINANCE_TARGET, str), "could not determine yfinance target"


def cur_timestamp(utc=True):
    if utc:
        return (
            datetime.utcnow()
            .replace(second=0, microsecond=0)
            .strftime("%Y-%m-%d %H:%M:%S")
        )
    else:
        return (
            datetime.now()
            .replace(second=0, microsecond=0)
            .strftime("%Y-%m-%d %H:%M:%S")
        )


### GENERAL ROUTES ###


@app.route("/")
def index():
    return "Financial-ELT hosting successful."


@app.route("/healthcheck", methods=["GET"])
def healthcheck():
    with app.app_context():
        return make_response("Healthcheck successful.", 200)


@app.route("/financial-elt", methods=["GET"])
def financial_elt():
    with app.app_context():
        return make_response("Financial-ELT is running.", 200)


###### tap yfinance routes ######


@app.route(f"/financial-elt/tap-yfinance-{ENVIRONMENT}", methods=["GET"])
def tap_yfinance(task_chunks=None):
    with app.app_context():
        start = time.monotonic()
        project_dir = "tap-yfinance"
        base_run_command = f"meltano --environment={ENVIRONMENT} el tap-yfinance target-{TAP_YFINANCE_TARGET}"

        if task_chunks is None:
            logging.info("Running meltano ELT without multiprocessing.")
            run_command = f"{base_run_command} --state-id tap_yfinance_{ENVIRONMENT}_{TAP_YFINANCE_TARGET}".split(
                " "
            )
            logging.info(f"Running command {run_command}")
            subprocess.run(run_command, cwd=os.path.join(app.root_path, project_dir))
        else:
            logging.info(
                f"Running meltano ELT using multiprocessing. Number of processes set to {os.getenv('TAP_YFINANCE_NUM_WORKERS')}."
            )

            processes = []

            for chunk in task_chunks:
                assert isinstance(
                    chunk, list
                ), "Invalid datatype task_chunks. Must be list when running multiprocessing."
                state_id = (
                    " ".join(chunk)
                    .replace("--select ", "")
                    .replace(" ", "__")
                    .replace(".*", "")
                )
                select_param = " ".join(chunk).replace(".*", "")
                run_command = (
                    f"{base_run_command} "
                    f"--state-id tap_yfinance_target_{TAP_YFINANCE_TARGET}_{ENVIRONMENT}_{state_id} {select_param}".split(
                        " "
                    )
                )

                process = mp.Process(
                    target=subprocess.run,
                    kwargs={
                        "args": run_command,
                        "cwd": os.path.join(app.root_path, project_dir),
                    },
                )

                process.daemon = True
                logging.info(f"Running command {run_command}")
                process.start()
                time.sleep(3)
                processes.append(process)

            for p in processes:
                p.join()

            logging.info(
                f"*** Completed process {process} --- run_commands: {task_chunks}"
            )

        total_seconds = time.monotonic() - start
        logging.info(
            f"*** ELT Process took {round(total_seconds, 2)} seconds ({round(total_seconds / 60, 2)} minutes, {round(total_seconds / 3600, 2)} hours) ***"
        )
        return make_response(
            f"Last ran project tap-yfinance-{ENVIRONMENT} target {TAP_YFINANCE_TARGET} at {cur_timestamp()}.",
            200,
        )
