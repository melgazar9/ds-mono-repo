from flask import Flask, make_response
from utils import *

DEBUG = False
ENVIRONMENT = os.getenv("ENVIRONMENT")
app = Flask(__name__)
app.url_map.strict_slashes = False

### GLOBALS ###

TAP_CONFIGS = {
    "tap-yfinance": {
        "file_target": os.getenv("TAP_YFINANCE_FILE_TARGET"),
        "db_target": os.getenv("TAP_YFINANCE_DB_TARGET"),
        "num_workers": int(os.getenv("TAP_YFINANCE_NUM_WORKERS")),
        "parallelism_method": os.getenv("TAP_YFINANCE_PARALLELISM_METHOD"),
        "semaphore": int(os.getenv("TAP_YFINANCE_MP_SEMAPHORE", "8")),
    },
    "tap-polygon": {
        "file_target": os.getenv("TAP_POLYGON_FILE_TARGET"),
        "db_target": os.getenv("TAP_POLYGON_DB_TARGET"),
        "num_workers": int(os.getenv("TAP_POLYGON_NUM_WORKERS")),
        "parallelism_method": os.getenv("TAP_POLYGON_PARALLELISM_METHOD"),
        "semaphore": int(os.getenv("TAP_POLYGON_MP_SEMAPHORE", "8")),
    },
}


for tap_cfg in os.getenv("FINANCIAL_ELT_TAPS_TO_RUN").split(","):
    assert isinstance(
        TAP_CONFIGS[tap_cfg]["db_target"], str
    ), f"could not determine {TAP_CONFIGS[tap_cfg]} db target"
    assert isinstance(
        TAP_CONFIGS[tap_cfg]["file_target"], str
    ), f"could not determine {TAP_CONFIGS[tap_cfg]} file target"


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


class MeltanoTap:
    def __init__(self, project_dir, num_workers, tap_name=None, target_name=None):
        self.num_workers = num_workers
        self.project_dir = project_dir
        self.tap_name = self.project_dir if tap_name is None else tap_name

        env_prefix = self.tap_name.upper().replace("-", "_")
        self.db_target = os.getenv(f"{env_prefix}_DB_TARGET")
        self.file_target = os.getenv(f"{env_prefix}_FILE_TARGET")
        if not self.db_target and not self.file_target:
            raise ValueError(
                f"You must set at least one of {env_prefix}_DB_TARGET or {env_prefix}_FILE_TARGET in your environment."
            )

        self.base_run_command = f"meltano --environment={ENVIRONMENT} el {self.tap_name}"
        self.cwd = os.path.join(app.root_path, project_dir)

        self.task_chunks = (
            get_task_chunks(num_workers, self.tap_name) if num_workers > 1 else None
        )

        # if DEBUG:
        #     self.task_chunks = self.task_chunks[-1:]

    def run_tap_single_threaded(self):
        logging.info("Running meltano ELT without multiprocessing.")

        tasks_dict = get_task_chunks(1, self.tap_name)
        results = []

        for target_type, target in [("file", self.file_target), ("db", self.db_target)]:
            if target and tasks_dict.get(target_type):
                for chunk in tasks_dict[target_type]:
                    select_param = " ".join(chunk).replace(".*", "")
                    select_id = (
                        select_param.replace("--select ", "").replace(" ", "_")
                        if select_param
                        else "unknown"
                    )
                    state_id = f"{self.tap_name.replace('-', '_')}_{ENVIRONMENT}_{target}__{select_id}"
                    run_command = (
                        f"{self.base_run_command} target-{target} --state-id {state_id} {select_param}"
                    ).split(" ")
                    command, return_code = run_meltano_task(
                        run_command=run_command, cwd=self.cwd
                    )
                    logging.info(f"command: {command} ---> return_code: {return_code}")
                    results.append((command, return_code))

        if results:
            logging.info("SINGLE-THREADED SUMMARY:")
            for command, return_code in results:
                logging.info(f"command: {command} ---> return_code: {return_code}")

    def run_tap_in_parallel(self):
        run_commands = get_run_commands(
            base_run_command=self.base_run_command,
            task_chunks_dict=self.task_chunks,
            tap_name=self.tap_name,
        )

        parallelism_env_var = (
            f"{self.tap_name.replace('-', '_').upper()}_PARALLELISM_METHOD"
        )
        parallelism_method = os.getenv(parallelism_env_var)

        assert isinstance(
            parallelism_method, str
        ), f"Must provide parallelism_method {parallelism_env_var} in .env."

        logging.info(
            f"Running meltano ELT {self.tap_name} using {parallelism_method}. Number of workers set to {self.num_workers}."
        )

        if parallelism_method.lower() in ["threadpool", "processpool"]:
            run_pool_task(
                run_commands=run_commands,
                cwd=self.cwd,
                num_workers=self.num_workers,
                pool_task=parallelism_method.lower(),
            )
        elif parallelism_method.lower() == "process":
            run_process_task(
                run_commands=run_commands,
                cwd=self.cwd,
                concurrency_semaphore=mp.Semaphore(
                    int(
                        os.getenv(
                            f"{self.tap_name.replace('-', '_').upper()}_MP_SEMAPHORE"
                        )
                    )
                ),
            )
        else:
            raise ValueError(
                f"Could not determine parallelism_method. It's currently set to {parallelism_method}"
            )

    def run_tap(self):
        if self.num_workers == 1:
            self.run_tap_single_threaded()
        elif self.num_workers > 1:
            self.run_tap_in_parallel()
        else:
            raise ValueError(
                f"Must provide configuration in .env for <TAP_NAME>_NUM_WORKERS "
                f"(e.g. {self.tap_name.replace('-', '_').upper()}_NUM_WORKERS)"
            )


def run_tap_route(tap_name):
    with app.app_context():
        start = time.monotonic()

        num_workers = int(os.getenv(f"{tap_name.upper().replace('-', '_')}_NUM_WORKERS"))
        target = os.getenv(f"{tap_name.upper().replace('-', '_')}_TARGET")

        tap = MeltanoTap(
            project_dir=tap_name,
            num_workers=num_workers,
            tap_name=tap_name,
            target_name=target,
        )

        tap.run_tap()

        total_seconds = time.monotonic() - start

        logging.info(
            f"*** ELT Process took {round(total_seconds, 2)} seconds "
            f"({round(total_seconds / 60, 2)} minutes,"
            f"{round(total_seconds / 3600, 2)} hours) ***"
        )

        return make_response(
            f"Last ran project {tap_name}-{ENVIRONMENT} target {target} at {cur_timestamp()}.",
            200,
        )


@app.route(f"/financial-elt/tap-yfinance-{ENVIRONMENT}", methods=["GET"])
def tap_yfinance():
    return run_tap_route("tap-yfinance")


@app.route(f"/financial-elt/tap-polygon-{ENVIRONMENT}", methods=["GET"])
def tap_polygon():
    return run_tap_route("tap-polygon")
