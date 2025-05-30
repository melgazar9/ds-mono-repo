import logging
import multiprocessing as mp
import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import lru_cache
from glob import glob
from queue import Empty

import yaml

ENVIRONMENT = os.getenv("ENVIRONMENT")
if ENVIRONMENT is None:
    raise ValueError(
        "Environment variable ENVIRONMENT is not set. Please set it to 'dev', 'staging', or 'production'."
    )

TIMEOUT_SECONDS = 43200  # 12 hours


def cur_timestamp(utc=True):
    if utc:
        return (
            datetime.utcnow()
            .replace(second=0, microsecond=0)
            .strftime("%Y-%m-%d %H:%M:%S")
        )
    else:
        return (
            datetime.now().replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        )


class SimpleCompletedProcess:
    def __init__(self, returncode):
        self.returncode = returncode


def get_task_chunks(num_tasks: int, tap_name):
    with open(f"{tap_name}/meltano.yml", "r") as meltano_cfg:
        cfg = yaml.safe_load(meltano_cfg)

    tasks = cfg.get("plugins", {}).get("extractors", [{}])[0].get("select", [])
    tasks = [f"--select {i}" for i in tasks]
    if not tasks:
        logging.error(
            f"No tasks found in {tap_name}/meltano.yml under plugins.extractors[0].select"
        )
        raise ValueError(f"No tasks to process found in {tap_name}/meltano.yml")

    tasks_per_chunk = len(tasks) // num_tasks
    remainder = len(tasks) % num_tasks

    chunks = []
    start_index = 0
    for i in range(num_tasks):
        chunk_size = tasks_per_chunk + (1 if i < remainder else 0)
        chunks.append(tasks[start_index : start_index + chunk_size])
        start_index += chunk_size
    chunks = [i for i in chunks if i]  # Filter out empty chunks
    return chunks


def run_pool_task(run_commands, cwd, num_workers, pool_task):
    if pool_task.lower() == "processpool":
        ExecutorClass = ProcessPoolExecutor
    elif pool_task.lower() == "threadpool":
        ExecutorClass = ThreadPoolExecutor
    else:
        raise ValueError(
            "Invalid value for pool_task. Must be 'processpool' or 'threadpool'."
        )

    cmd_return_codes = {}
    with ExecutorClass(max_workers=num_workers) as executor:
        futures = []
        for run_command in run_commands:
            futures.append(
                executor.submit(
                    run_meltano_task,
                    run_command,
                    cwd=cwd,
                )
            )

        try:
            for future in as_completed(futures, timeout=TIMEOUT_SECONDS):
                try:
                    command, return_code = future.result()
                    logging.info(f"command: {command} ---> return_code: {return_code}")
                    cmd_return_codes[str(command)] = return_code
                except Exception as e:
                    logging.error(f"A task in the pool failed: {e}")
        except TimeoutError:
            logging.error(
                "Overall pool execution timed out after 8 hours. Attempting to terminate remaining processes."
            )
            if isinstance(executor, ProcessPoolExecutor):
                executor.shutdown(wait=False, cancel_futures=True)
            elif isinstance(executor, ThreadPoolExecutor):
                executor.shutdown(wait=False, cancel_futures=True)
            raise

    logging.info("\n".join([f"{k} ---> {v}" for k, v in cmd_return_codes.items()]))
    return cmd_return_codes


def run_process_task(run_commands, cwd, concurrency_semaphore):
    processes = []
    cmd_return_codes = {}
    return_queue = mp.Queue()
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
        except Exception as e:
            logging.error(f"Failed to start process for {run_command}: {e}")
            continue

    for process in processes:
        try:
            process.join(timeout=28800)
            if process.exitcode is None:
                logging.error(
                    f"Process {process.pid} for command {process._kwargs.get('run_command')}"
                    f"timed out during join. Terminating."
                )
                process.terminate()
                process.join(timeout=5)
        except Exception as e:
            logging.error(
                f"Error joining process {process.pid} -- command {process._kwargs.get('run_command')}: {e}. Terminating."
            )
            process.terminate()
            process.join(timeout=5)

    # Collect results with timeout to avoid queue deadlock if a process failed to put its result
    try:
        for _ in range(len(run_commands)):
            try:
                command, return_code = return_queue.get(timeout=60)  # 60-second timeout for getting from queue
                logging.info(f"SUBPROCESS COMMAND: {command} FINISHED WITH RETURN CODE ---> return_code: {return_code}")
                cmd_return_codes[str(command)] = return_code
            except Empty:
                logging.error(
                    "Queue timed out -- not all results collected."
                    "Possible process failure or unhandled exception in child."
                )
                break  # Stop trying to get from queue if it's empty
    finally:
        return_queue.close()
        return_queue.join_thread()

    logging.info("\n".join([f"{k} ---> {v}" for k, v in cmd_return_codes.items()]))
    return cmd_return_codes


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info(f"\n*** Running environment {ENVIRONMENT}. ***\n")
    return


def clean_old_logs(log_dir, max_files=100):
    if not os.path.exists(log_dir):
        return

    log_files = sorted(
        glob(os.path.join(log_dir, "*.log")), key=os.path.getmtime, reverse=True
    )

    for old_log in log_files[max_files:]:
        try:
            os.remove(old_log)
        except Exception as e:
            logging.warning(f"Failed to remove old log file {old_log}: {e}")


def critical_shutdown_handler(signum, frame):
    logging.warning(f"Received signal {signum}. Shutting down...")

    # Terminate all active child processes managed by multiprocessing
    for process in mp.active_children():
        logging.info(
            f"Terminating child process {process.pid} (command: {process._kwargs.get('run_command')})"
        )
        process.terminate()
        process.join(timeout=5)
    sys.exit(1)


def get_run_commands(base_run_command, task_chunks, tap_name, target_name):
    run_commands = []
    for chunk in task_chunks:
        assert isinstance(chunk, list), "Invalid datatype task_chunks. Must be list."
        state_id = (
            " ".join(chunk).replace("--select ", "").replace(" ", "__").replace(".*", "")
        )
        select_param = " ".join(chunk).replace(".*", "")
        run_command = (
            f"{base_run_command} "
            f"--state-id {tap_name.replace('tap-', 'tap_')}_"
            f"target_{target_name.replace('target-', 'target_')}_"
            f"{ENVIRONMENT}_{state_id} "
            f"{select_param}".split(" ")
        )
        run_commands.append(run_command)
    return run_commands


@lru_cache(maxsize=None)
def find_monorepo_root(start_path=None):
    # Markers ordered by priority (higher first)
    markers = [
        ".git",
        "docker-compose.yml",
        "pyproject.toml",
        "package.json",
        # Lower priority markers after
        "state.json",
        "tap-polygon",
        "tap-yfinance",
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
                # Don't return immediately, keep searching upward for better priority
                break
        parent_dir = os.path.realpath(os.path.join(current_dir, ".."))
        if parent_dir == current_dir:
            # reached root filesystem
            break
        current_dir = parent_dir
    if not candidates:
        return None
    # Return dir with marker of highest priority (lowest index)
    best = min(candidates, key=lambda x: x[0])
    return best[1]


def execute_command(run_command, cwd, concurrency_semaphore=None):
    """Runs a command with optional concurrency semaphore."""
    if concurrency_semaphore:
        with concurrency_semaphore:
            return execute_command_stg(run_command, cwd)
    return execute_command_stg(run_command, cwd)


def execute_command_stg(run_command, cwd):
    """
    Executes a shell command, redirecting stdout/stderr to files to prevent deadlocks
    with large outputs. Raises exceptions for timeouts or non-zero exit codes.
    """
    project_name = run_command[3]
    subprocess_log_dir = os.path.join(find_monorepo_root(), "logs", f"{project_name}")
    os.makedirs(subprocess_log_dir, exist_ok=True)

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

    try:
        # Open log files for writing. Any errors here (e.g., permissions, disk full)
        # will propagate as standard Python I/O exceptions.
        with open(stdout_log_path, "w", encoding="utf-8") as stdout_file, open(
            stderr_log_path, "w", encoding="utf-8"
        ) as stderr_file:

            process = subprocess.Popen(
                run_command,
                cwd=cwd,
                stdout=stdout_file,
                stderr=stderr_file,
                shell=False,
            )

            # Wait for the process to complete with a timeout.
            # This will raise subprocess.TimeoutExpired if the process doesn't finish in time.
            return_code = process.wait(timeout=TIMEOUT_SECONDS)

            seconds_taken = time.monotonic() - start_time

            # Check the return code. If non-zero, raise CalledProcessError.
            # This replicates the `check=True` behavior of subprocess.run.
            if return_code != 0:
                logging.error(
                    f"Command {' '.join(run_command)} failed with return code {return_code}. \n "
                    f"Subprocess took {round(seconds_taken, 2)} seconds ({round(seconds_taken / 60, 2)} minutes, "
                    f"{round(seconds_taken / 3600, 2)} hours) to fail.\n"
                    f"Full STDOUT in: {stdout_log_path}\n"
                    f"Full STDERR in: {stderr_log_path}"
                )

                raise subprocess.CalledProcessError(
                    return_code,
                    run_command,
                    output=f"Output in {stdout_log_path}",
                    stderr=f"Errors in {stderr_log_path}",
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
        if process and process.poll() is None:  # Check if process is still running
            process.kill()
            process.wait(timeout=5)
        raise e

    except FileNotFoundError as e:
        logging.error(
            f"Command not found: '{run_command[0]}'. Please ensure it's in your system's PATH. Error: {e}"
        )
        raise e

    except Exception as e:
        seconds_taken = time.monotonic() - start_time
        logging.error(
            f"An unexpected error occurred while executing command {' '.join(run_command)}: {e}. "
            f"Time elapsed: {round(seconds_taken, 2)} seconds."
        )
        if process and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        raise e


def run_meltano_task(run_command, cwd, concurrency_semaphore=None, return_queue=None):
    """
    Runs the Meltano task, handling exceptions from execute_command_stg and
    putting the result (or error code) into the queue.
    """
    return_code = 1  # Default to a generic error code

    try:
        result = execute_command(
            run_command=run_command, cwd=cwd, concurrency_semaphore=concurrency_semaphore
        )
        return_code = result.returncode
    except subprocess.TimeoutExpired:
        logging.error(f"Meltano task {' '.join(run_command)} timed out.")
        return_code = 124
    except subprocess.CalledProcessError as e:
        logging.error(
            f"Meltano task {' '.join(run_command)} failed with exit code {e.returncode}."
        )
        return_code = e.returncode
    except FileNotFoundError:
        logging.error(
            f"Meltano executable not found for task {' '.join(run_command)}. Please check system PATH."
        )
        return_code = 127  # Standard exit code for command not found
    except Exception as e:
        logging.error(
            f"An unexpected error occurred during Meltano task {' '.join(run_command)}: {e}"
        )
        return_code = 1

    if return_queue:
        try:
            return_queue.put(
                (run_command, return_code), timeout=5
            )  # Add timeout to put as well
        except Exception as e:
            logging.error(
                f"Failed to put result in queue for {' '.join(run_command)}: {e}"
            )

    return run_command, return_code
