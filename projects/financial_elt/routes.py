from flask import Flask, make_response, request, redirect, Blueprint, render_template
import os
import subprocess
from datetime import datetime
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import yaml


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


def get_task_chunks(num_tasks: int):
    with open("tap-yfinance/meltano.yml", "r") as meltano_cfg:
        cfg = yaml.safe_load(meltano_cfg)

    tasks = cfg.get("plugins").get("extractors")[0].get("select")
    tasks = [f"--select {i}" for i in tasks]
    tasks_per_chunk = len(tasks) // num_tasks
    remainder = len(tasks) % num_tasks

    chunks = []
    start_index = 0
    for i in range(num_tasks):
        chunk_size = tasks_per_chunk + (1 if i < remainder else 0)
        chunks.append(tasks[start_index : start_index + chunk_size])
        start_index += chunk_size
    return chunks


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
def tap_yfinance():
    with app.app_context():
        start = time.monotonic()
        task_chunks = get_task_chunks(int(os.getenv("TAP_YFINANCE_NUM_WORKERS")))
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

            with ThreadPoolExecutor(
                max_workers=int(os.getenv("TAP_YFINANCE_NUM_WORKERS", 16))
            ) as executor:
                futures = []

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

                    futures.append(
                        executor.submit(
                            subprocess.run,
                            run_command,
                            cwd=os.path.join(app.root_path, project_dir),
                        )
                    )

                for future in futures:
                    future.result()

                logging.info(f"Running command {run_command}")

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
