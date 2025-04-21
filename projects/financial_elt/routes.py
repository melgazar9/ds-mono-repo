from utils import *

app = Flask(__name__)

app.url_map.strict_slashes = False

### GLOBALS ###

ENVIRONMENT = os.getenv("ENVIRONMENT")
TAP_YFINANCE_TARGET = os.getenv("TAP_YFINANCE_TARGET")

assert isinstance(TAP_YFINANCE_TARGET, str), "could not determine yfinance target"


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


###### tap yfinance route ######

"""
  Meltano tap-yfinance tests on 10 tickers
    - Test 0 — BENCHMARK
        - Test a direct single meltano el run —> meltano el tap-yfinance target-jsonl
        - Time taken: 15.96 minutes
    - Test 1
        - NUM_WORKERS = 1 (same as above, except use hosting and gcp storage (other parallelization params are irrelevant)
        - Outcome: 
            - Total time taken: 32.81 minutes
    - Test 2
        - NUM_WORKERS = 67
        - TAP_YFINANCE_PARALLELISM_METHOD=process
        - TAP_YFINANCE_MP_SEMAPHORE=67
        - Outcome:
            - Total time taken: 18.63 minutes
            - Took 16 minutes to initialize —> read from cloud storage step (first ticker read)
            - => time taken after initialization was less than 3 minutes
    - Test 3
        - NUM_WORKERS = 67
        - TAP_YFINANCE_PARALLELISM_METHOD=process
        - TAP_YFINANCE_MP_SEMAPHORE=20
        - Outcome:
            - Total time taken: 18.38 minutes
    - Test 4
        - NUM_WORKERS = 67
        - TAP_YFINANCE_PARALLELISM_METHOD=processpool
        - TAP_YFINANCE_MP_SEMAPHORE => IRRELEVANT
        - Outcome:
            - Total time taken: 18.49 minutes
    - Test 5
        - NUM_WORKERS = 32
        - TAP_YFINANCE_PARALLELISM_METHOD=processpool
        - TAP_YFINANCE_MP_SEMAPHORE => IRRELEVANT
        - Outcome:
            - Total time taken: 10.95 minutes
    - Test 6
        - NUM_WORKERS = 67
        - TAP_YFINANCE_PARALLELISM_METHOD=threadpool
        - TAP_YFINANCE_MP_SEMAPHORE => IRRELEVANT
        - Outcome:
            - Total time taken: 18.92 minutes
    - Test 7
        - NUM_WORKERS = 32
        - TAP_YFINANCE_PARALLELISM_METHOD=threadpool
        - TAP_YFINANCE_MP_SEMAPHORE => IRRELEVANT
        - Outcome:
            - Total time taken: 10.86 minutes
    - Test 8
        - NUM_WORKERS = 200
        - TAP_YFINANCE_PARALLELISM_METHOD=threadpool
        - TAP_YFINANCE_MP_SEMAPHORE => IRRELEVANT
        - Outcome:
            - Total time taken: 18.5 minutes
    - Test 9
        - NUM_WORKERS = 16
        - TAP_YFINANCE_PARALLELISM_METHOD=threadpool
        - TAP_YFINANCE_MP_SEMAPHORE => IRRELEVANT
        - Outcome:
            - Total time taken: 9.56 minutes
    - Test 10
        - NUM_WORKERS = 16
        - TAP_YFINANCE_PARALLELISM_METHOD=processpool
        - TAP_YFINANCE_MP_SEMAPHORE => IRRELEVANT
        - Outcome:
            - Total time taken: 9.50 minutes
"""


@app.route(f"/financial-elt/tap-yfinance-{ENVIRONMENT}", methods=["GET"])
def tap_yfinance():
    with app.app_context():
        start = time.monotonic()
        num_workers = int(os.getenv("TAP_YFINANCE_NUM_WORKERS"))
        project_dir = "tap-yfinance"
        base_run_command = f"meltano --environment={ENVIRONMENT} el tap-yfinance target-{TAP_YFINANCE_TARGET} --force"
        cwd = os.path.join(app.root_path, project_dir)
        task_chunks = get_task_chunks(num_workers) if num_workers > 1 else None

        if task_chunks is None:
            logging.info("Running meltano ELT without multiprocessing.")
            run_command = f"{base_run_command} --state-id tap_yfinance_{ENVIRONMENT}_{TAP_YFINANCE_TARGET}".split(
                " "
            )
            run_meltano_task(run_command=run_command, cwd=cwd)

            logging.info(f"Running command {run_command}")
            subprocess.run(run_command, cwd=cwd, check=True)
        else:
            run_commands = get_run_commands(
                base_run_command=base_run_command,
                task_chunks=task_chunks,
                target=TAP_YFINANCE_TARGET,
            )

            parallelism_method = os.getenv("TAP_YFINANCE_PARALLELISM_METHOD")
            assert isinstance(
                parallelism_method, str
            ), f"parallelism_method is not a str, it is set to {parallelism_method}"

            logging.info(
                f"Running meltano ELT using approach {parallelism_method}. Number of workers set to {num_workers}."
            )

            if parallelism_method.lower() in ["threadpool", "processpool"]:
                run_pool_task(
                    run_commands=run_commands,
                    cwd=cwd,
                    num_workers=num_workers,
                    pool_task=parallelism_method.lower(),
                )

            elif parallelism_method.lower() == "process":
                run_process_task(
                    run_commands=run_commands,
                    cwd=cwd,
                    concurrency_semaphore=mp.Semaphore(
                        int(os.getenv("TAP_YFINANCE_MP_SEMAPHORE"))
                    ),
                )
            else:
                raise ValueError(
                    f"Could not determine parallelism_method. It's currently set to {parallelism_method}"
                )

        total_seconds = time.monotonic() - start
        logging.info(
            f"*** ELT Process took {round(total_seconds, 2)} seconds ({round(total_seconds / 60, 2)} minutes, {round(total_seconds / 3600, 2)} hours) ***"
        )

        return make_response(
            f"Last ran project tap-yfinance-{ENVIRONMENT} target {TAP_YFINANCE_TARGET} at {cur_timestamp()}.",
            200,
        )
