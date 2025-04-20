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


@app.route(f"/financial-elt/tap-yfinance-{ENVIRONMENT}", methods=["GET"])
def tap_yfinance():
    with app.app_context():
        start = time.monotonic()
        num_workers = int(os.getenv("TAP_YFINANCE_NUM_WORKERS"))
        task_chunks = get_task_chunks(num_workers)
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
            parallelism_method = os.getenv("TAP_YFINANCE_PARALLELISM_METHOD")
            assert isinstance(
                parallelism_method, str
            ), f"parallelism_method is not a str, it is set to {parallelism_method}"

            logging.info(
                f"Running meltano ELT using approach {parallelism_method}. Number of workers set to {num_workers}."
            )

            if parallelism_method.lower() == "threadpool":
                logging.info(f"Using ThreadPoolExecutor with {num_workers} workers.")

                with ThreadPoolExecutor(max_workers=num_workers) as executor:
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

                logging.info(f"*** Completed run_commands: {task_chunks}")

            elif parallelism_method.lower() == "processes":
                processes = []
                concurrency_semaphore = mp.Semaphore(
                    int(os.getenv("TAP_YFINANCE_MP_SEMAPHORE"))
                )

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
                        target=run_meltano_task,
                        kwargs={
                            "run_command": run_command,
                            "concurrency_semaphore": concurrency_semaphore,
                            "cwd": os.path.join(app.root_path, project_dir),
                        },
                    )

                    logging.info(f"Running command {run_command}")
                    process.start()
                    processes.append(process)

                for p in processes:
                    p.join()

                logging.info(
                    f"*** Completed process {process} --- run_commands: {task_chunks}"
                )
            else:
                raise ValueError(
                    f"Could not determine parallelism_method. It's currenctly set to {parallelism_method}"
                )

        total_seconds = time.monotonic() - start
        logging.info(
            f"*** ELT Process took {round(total_seconds, 2)} seconds ({round(total_seconds / 60, 2)} minutes, {round(total_seconds / 3600, 2)} hours) ***"
        )

        return make_response(
            f"Last ran project tap-yfinance-{ENVIRONMENT} target {TAP_YFINANCE_TARGET} at {cur_timestamp()}.",
            200,
        )
