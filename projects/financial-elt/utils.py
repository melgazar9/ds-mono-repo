import logging
import multiprocessing as mp
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import lru_cache
from glob import glob

import yaml

ENVIRONMENT = os.getenv("ENVIRONMENT")
if ENVIRONMENT is None:
    raise ValueError("Environment variable ENVIRONMENT is not set. Please set it to 'dev', 'staging', or 'production'.")
TIMEOUT_SECONDS = 777600  # 9 days just for safety, but should not be needed... set this high because of the first stream


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def cur_timestamp(utc=True):
    dt = datetime.utcnow() if utc else datetime.now()
    return dt.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


class SimpleCompletedProcess:
    def __init__(self, returncode):
        self.returncode = returncode


def get_task_chunks(num_tasks: int, tap_name):
    with open(f"{tap_name}/meltano.yml", "r") as meltano_cfg:
        cfg = yaml.safe_load(meltano_cfg)

    all_tasks = [str(i) for i in cfg.get("plugins", {}).get("extractors", [{}])[0].get("select", [])]
    all_tables = set([i.split(".", 1)[0] for i in all_tasks])

    db_env_var = f"{tap_name.upper().replace('-', '_')}_TARGET_DB_TABLES"
    file_env_var = f"{tap_name.upper().replace('-', '_')}_TARGET_FILE_TABLES"
    db_tables_raw = os.getenv(db_env_var, "").strip()
    file_tables_raw = os.getenv(file_env_var, "").strip()

    db_tables_set = (
        set(x.strip() for x in db_tables_raw.split(",") if x.strip()) if db_tables_raw and db_tables_raw != "*" else None
    )
    file_tables_set = (
        set(x.strip() for x in file_tables_raw.split(",") if x.strip()) if file_tables_raw and file_tables_raw != "*" else None
    )

    if db_tables_raw == "*" and file_tables_raw == "*":
        raise ValueError("Both DB and FILE target tables are set to '*', cannot send all tables to both targets.")

    allowed_tables_by_target = {}

    if db_tables_raw == "*":
        allowed_tables_by_target["db"] = all_tables - (file_tables_set or set())
    else:
        allowed_tables_by_target["db"] = db_tables_set or set()

    if file_tables_raw == "*":
        allowed_tables_by_target["file"] = all_tables - (db_tables_set or set())
    else:
        allowed_tables_by_target["file"] = file_tables_set or set()

    # Check for overlaps
    overlap = allowed_tables_by_target["file"] & allowed_tables_by_target["db"]
    if overlap:
        raise ValueError(f"These tables are assigned to both file and db targets: {sorted(overlap)}")

    tasks_set = set(all_tasks)
    for target_type, env_tables in allowed_tables_by_target.items():
        missing_in_meltano = {
            table
            for table in env_tables
            if not any(select_item == table or select_item.startswith(f"{table}.") for select_item in tasks_set)
        }
        if missing_in_meltano:
            logging.warning(
                f"[{tap_name}] WARNING: These tables in {target_type} env var NOT found in meltano.yml:"
                f"{sorted(missing_in_meltano)}"
            )

    all_env_tables = set().union(*allowed_tables_by_target.values())
    missing_in_env = {select_item for select_item in tasks_set if select_item.split(".", 1)[0] not in all_env_tables}
    if missing_in_env:
        logging.warning(
            f"[{tap_name}] WARNING: These meltano.yml select entries are NOT in any *_TARGET_*_TABLES"
            f"env var: {sorted(missing_in_env)}"
        )

    chunks_by_target = {}
    for target_type in ["file", "db"]:
        allowed_tables = allowed_tables_by_target[target_type]
        if not allowed_tables:
            continue
        filtered_tasks = [
            f"--select {i}" for i in all_tasks if any(tbl == i or i.startswith(f"{tbl}.") for tbl in allowed_tables)
        ]
        if not filtered_tasks:
            continue

        if num_tasks == 1:
            chunks = [[t] for t in filtered_tasks]
        else:
            tasks_per_chunk = len(filtered_tasks) // num_tasks
            remainder = len(filtered_tasks) % num_tasks
            chunks = []
            start_index = 0
            for j in range(num_tasks):
                chunk_size = tasks_per_chunk + (1 if j < remainder else 0)
                chunks.append(filtered_tasks[start_index : start_index + chunk_size])
                start_index += chunk_size
            chunks = [i for i in chunks if i]

        chunks_by_target[target_type] = chunks
    return chunks_by_target


def run_pool_task(run_commands, cwd, num_workers, pool_task):
    ExecutorClass = {"processpool": ProcessPoolExecutor, "threadpool": ThreadPoolExecutor}[pool_task.lower()]
    cmd_return_codes = {}
    with ExecutorClass(max_workers=num_workers) as executor:
        futures = [executor.submit(run_meltano_task, run_command, cwd=cwd) for run_command in run_commands]
        try:
            for future in as_completed(futures, timeout=TIMEOUT_SECONDS):
                try:
                    command, return_code = future.result()
                    logging.info(f"command: {command} ---> return_code: {return_code}")
                    cmd_return_codes[str(command)] = return_code
                except Exception as e:
                    logging.error(f"A task in the pool failed: {e}")
        except TimeoutError:
            logging.error("Overall pool execution timed out after 8 hours. Attempting to terminate remaining processes.")
            executor.shutdown(wait=False, cancel_futures=True)
            raise
    logging.info("\n".join([f"{k} ---> {v}" for k, v in cmd_return_codes.items()]))
    return cmd_return_codes


def run_process_task(run_commands, cwd, concurrency_semaphore):
    processes = []
    cmd_return_codes = {}
    return_queue = mp.Queue()
    process_map = {}

    for run_command in run_commands:
        process = mp.Process(
            target=run_meltano_task,
            kwargs={
                "run_command": run_command,
                "concurrency_semaphore": concurrency_semaphore,
                "cwd": cwd,
                "return_queue": return_queue,
            },
        )
        logging.info(f"Running command {run_command}")
        try:
            process.start()
            processes.append(process)
            process_map[process.pid] = process
        except Exception as e:
            logging.error(f"Failed to start process for {run_command}: {e}")
            continue

    num_finished = 0
    total = len(processes)
    joined_pids = set()

    while num_finished < total:
        try:
            command, return_code = return_queue.get(timeout=3)
            logging.info(f"SUBPROCESS COMMAND: {command} FINISHED WITH RETURN CODE ---> return_code: {return_code}")
            cmd_return_codes[str(command)] = return_code
            num_finished += 1
        except queue.Empty:
            pass

        for process in processes:
            if process.pid not in joined_pids and not process.is_alive():
                process.join(timeout=1)
                joined_pids.add(process.pid)

    return_queue.close()
    return_queue.join_thread()
    logging.info("\n".join([f"{k} ---> {v}" for k, v in cmd_return_codes.items()]))
    return cmd_return_codes


def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler(sys.stdout)]
    )
    logging.info(f"\n*** Running environment {ENVIRONMENT}. ***\n")
    return


def clean_old_logs(log_dir, max_files=100):
    if not os.path.exists(log_dir):
        return
    log_files = sorted(glob(os.path.join(log_dir, "*.log")), key=os.path.getmtime, reverse=True)
    for old_log in log_files[max_files:]:
        try:
            os.remove(old_log)
        except Exception as e:
            logging.warning(f"Failed to remove old log file {old_log}: {e}")


def critical_shutdown_handler(signum, frame):
    logging.warning(f"Received signal {signum}. Shutting down...")
    for process in mp.active_children():
        logging.info(f"Terminating child process {process.pid} (command: {process._kwargs.get('run_command')})")
        process.terminate()
        process.join(timeout=5)
    sys.exit(1)


def get_run_commands(base_run_command: str, task_chunks_dict: dict, tap_name: str):
    """
    Flatten the dictionary into a list of commands. Parallelism is automatically handled by the MeltanoTap class.
    We don't want to mix two different targets in the same run_commands list. This is handled here and get_task_chunks.

    Returns
    -------
    list of meltano run commands
    """
    run_commands = []
    tap_env_prefix = tap_name.upper().replace("-", "_")
    target_env_vars = {"file": f"{tap_env_prefix}_FILE_TARGET", "db": f"{tap_env_prefix}_DB_TARGET"}
    for target_type, task_chunks in task_chunks_dict.items():
        if not task_chunks:
            logging.info(
                f"No tasks for target type: '{target_type}' in {tap_name}. Task chunks: {task_chunks}."
                f"Skipping target-type {target_type}."
            )
            continue
        target_name = os.getenv(target_env_vars[target_type])
        if not target_name:
            raise ValueError(f"Missing environment variable: {target_env_vars[target_type]}")
        run_command = f"{base_run_command} target-{target_name}"
        for chunk in task_chunks:
            assert isinstance(chunk, list), "Invalid datatype task_chunks. Must be list."
            state_id = " ".join(chunk).replace("--select ", "").replace(" ", "__").replace(".*", "")
            select_param = " ".join(chunk).replace(".*", "")
            cmd = (
                f"{run_command} "
                f"--state-id {tap_name.replace('tap-', 'tap_')}_"
                f"target_{target_name}_"
                f"{ENVIRONMENT}__{state_id} "
                f"{select_param}".split(" ")
            )
            run_commands.append(cmd)
    return run_commands


@lru_cache(maxsize=None)
def find_monorepo_root(start_path=None):
    markers = [
        ".git",
        "docker-compose.yml",
        "pyproject.toml",
        "package.json",
        "state.json",
        "tap-polygon",
        "tap-yfinance",
        "tap-yahooquery",
        "tap-fmp",
        "legacy",
        "tests",
    ]
    if start_path is None:
        start_path = os.getcwd()
    current_dir = os.path.realpath(start_path)
    candidates = []
    while True:
        for idx, marker in enumerate(markers):
            if os.path.exists(os.path.join(current_dir, marker)):
                candidates.append((idx, current_dir))
                break
        parent_dir = os.path.realpath(os.path.join(current_dir, ".."))
        if parent_dir == current_dir:
            break
        current_dir = parent_dir
    if not candidates:
        return None
    best = min(candidates, key=lambda x: x[0])
    return best[1]


def drain_stream(stream, log_path):
    with open(log_path, "a", buffering=1) as f:
        for line in iter(stream.readline, ""):
            if not line:
                break
            f.write(line)
    stream.close()


def execute_command(run_command, cwd, concurrency_semaphore=None):
    if concurrency_semaphore:
        with concurrency_semaphore:
            return execute_command_stg(run_command, cwd)
    return execute_command_stg(run_command, cwd)


def execute_command_stg(run_command, cwd):
    """
    Executes a shell command, redirecting stdout/stderr directly to files for deadlock-free operation.
    Each run gets its own temp MELTANO_PROJECT_ROOT for lock/deadlock prevention.
    .meltano/state is symlinked to the shared, persistent state directory.
    """
    project_name = run_command[3]
    subprocess_log_dir = os.path.join(find_monorepo_root(), "logs", f"{project_name}")
    ensure_dir(subprocess_log_dir)

    identifier_parts = ["meltano"]
    tap_name = None
    environment = None
    selected_streams = []

    i = 0
    while i < len(run_command):
        arg = run_command[i]
        if arg.startswith("tap-") and "target-" not in arg and "--state-id" not in arg:
            tap_name = arg.replace("-", "_")
        elif arg.startswith("--environment="):
            environment = arg.split("=")[1]
        elif arg == "--select":
            if i + 1 < len(run_command):
                selected_streams.append(run_command[i + 1])
                i += 1
        i += 1

    if tap_name:
        identifier_parts.append(tap_name)
    if environment:
        identifier_parts.append(environment)
    if selected_streams:
        identifier_parts.append("__".join(selected_streams))

    command_identifier = "__".join(identifier_parts)
    stdout_log_path = os.path.join(subprocess_log_dir, f"{command_identifier}_stdout.log")
    stderr_log_path = os.path.join(subprocess_log_dir, f"{command_identifier}_stderr.log")
    logging.info(f"Executing command: {' '.join(run_command)}")
    logging.info(f"Stdout redirected to: {stdout_log_path}")
    logging.info(f"Stderr redirected to: {stderr_log_path}")

    start_time = time.monotonic()
    process = None

    with tempfile.TemporaryDirectory(prefix="meltano_job_") as tmp_project_dir:
        # Copy everything EXCEPT .meltano
        for item in os.listdir(cwd):
            s = os.path.join(cwd, item)
            d = os.path.join(tmp_project_dir, item)
            if item == ".meltano":
                continue  # handled below
            try:
                if os.path.isdir(s):
                    os.symlink(s, d, target_is_directory=True)
                else:
                    os.symlink(s, d)
            except Exception:
                if os.path.isdir(s):
                    shutil.copytree(s, d, symlinks=True)
                else:
                    shutil.copy2(s, d)
        # Copy .meltano (except state), symlink .meltano/state to cwd .meltano/state
        meltano_src = os.path.join(cwd, ".meltano")
        meltano_dst = os.path.join(tmp_project_dir, ".meltano")
        shutil.copytree(meltano_src, meltano_dst, symlinks=True, ignore=shutil.ignore_patterns("state"))
        # Remove the empty/newly created .meltano/state (if it exists), then symlink it to the real state dir
        meltano_state_dst = os.path.join(meltano_dst, "state")
        meltano_state_src = os.path.join(meltano_src, "state")
        try:
            if os.path.islink(meltano_state_dst) or os.path.isfile(meltano_state_dst):
                os.unlink(meltano_state_dst)
            elif os.path.isdir(meltano_state_dst):
                shutil.rmtree(meltano_state_dst)
        except FileNotFoundError:
            pass
        os.symlink(meltano_state_src, meltano_state_dst, target_is_directory=True)

        env = os.environ.copy()
        env["MELTANO_PROJECT_ROOT"] = tmp_project_dir

        try:
            with open(stdout_log_path, "w") as out, open(stderr_log_path, "w") as err:
                process = subprocess.Popen(
                    run_command, cwd=tmp_project_dir, stdout=out, stderr=err, shell=False, close_fds=True, env=env
                )

                return_code = process.wait(timeout=TIMEOUT_SECONDS)
                seconds_taken = time.monotonic() - start_time

                if return_code != 0:
                    logging.error(
                        f"Command {' '.join(run_command)} failed with return code {return_code}. \n "
                        f"Subprocess took {round(seconds_taken, 2)} seconds ({round(seconds_taken / 60, 2)} minutes, "
                        f"{round(seconds_taken / 3600, 2)} hours) to fail.\n"
                        f"Full STDOUT in: {stdout_log_path}\n"
                        f"Full STDERR in: {stderr_log_path}"
                    )
                    raise subprocess.CalledProcessError(
                        return_code, run_command, output=f"Output in {stdout_log_path}", stderr=f"Errors in {stderr_log_path}"
                    )

                logging.info(
                    f"Command {' '.join(run_command)} completed successfully with return code {return_code}. \n"
                    f"Subprocess took {round(seconds_taken, 2)} seconds ({round(seconds_taken / 60, 2)} minutes,"
                    f"{round(seconds_taken / 3600, 2)} hours) to succeed.\n"
                    f"Full STDOUT in: {stdout_log_path}\n"
                    f"Full STDERR in: {stderr_log_path}"
                )
                return SimpleCompletedProcess(return_code)

        except subprocess.TimeoutExpired as e:
            seconds_taken = time.monotonic() - start_time
            logging.error(
                f"Command {' '.join(run_command)} timed out after {round(seconds_taken, 2)} seconds. "
                f"Full logs in: {stdout_log_path} and {stderr_log_path}"
            )
            if process and process.poll() is None:
                process.kill()
                process.wait(timeout=60)
            raise e

        except FileNotFoundError as e:
            logging.error(f"Command not found: '{run_command[0]}'. Please ensure it's in your system's PATH. Error: {e}")
            raise e

        except Exception as e:
            seconds_taken = time.monotonic() - start_time
            logging.error(
                f"An unexpected error occurred while executing command {' '.join(run_command)}: {e}. "
                f"Time elapsed: {round(seconds_taken, 2)} seconds."
            )
            if process and process.poll() is None:
                process.kill()
                process.wait(timeout=60)
            raise e


def run_meltano_task(run_command, cwd, concurrency_semaphore=None, return_queue=None):
    """
    Runs the Meltano task, handling exceptions from execute_command_stg and
    putting the result (or error code) into the queue.
    """
    return_code = 1
    try:
        result = execute_command(run_command=run_command, cwd=cwd, concurrency_semaphore=concurrency_semaphore)
        return_code = result.returncode
    except subprocess.TimeoutExpired:
        logging.error(f"Meltano task {' '.join(run_command)} timed out.")
        return_code = 124
    except subprocess.CalledProcessError as e:
        logging.error(f"Meltano task {' '.join(run_command)} failed with exit code {e.returncode}.")
        return_code = e.returncode
    except FileNotFoundError:
        logging.error(f"Meltano executable not found for task {' '.join(run_command)}. Please check system PATH.")
        return_code = 127  # Standard exit code for command not found
    except Exception as e:
        logging.error(f"An unexpected error occurred during Meltano task {' '.join(run_command)}: {e}")
        return_code = 1

    if return_queue:
        try:
            return_queue.put((run_command, return_code), timeout=60)
        except Exception as e:
            logging.error(f"Failed to put result in queue for {' '.join(run_command)}: {e}")
    return run_command, return_code
