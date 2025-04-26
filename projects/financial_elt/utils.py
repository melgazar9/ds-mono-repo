from flask import Flask, make_response, request, redirect, Blueprint, render_template
import os
import subprocess
from datetime import datetime
import logging
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import yaml
import multiprocessing as mp
from functools import partial
import sys

ENVIRONMENT = os.getenv("ENVIRONMENT")


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
    chunks = [i for i in chunks if i != []]
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

    with ExecutorClass(max_workers=num_workers) as executor:
        futures = []
        cmd_return_codes = {}
        for run_command in run_commands:
            futures.append(
                executor.submit(
                    run_meltano_task,
                    run_command,
                    cwd=cwd,
                )
            )

        for future in as_completed(futures):
            command, return_code = future.result()
            logging.info(
                f"""
                command: {command} ---> return_code: {return_code}
                """
            )
            cmd_return_codes[str(command)] = return_code

    logging.info("\n".join([f"{k} ---> {v}" for k, v in cmd_return_codes.items()]))
    return


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
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    for _ in run_commands:
        command, return_code = return_queue.get()
        cmd_return_codes[str(command)] = return_code

    logging.info("\n".join([f"{k} ---> {v}" for k, v in cmd_return_codes.items()]))
    return


def setup_logging():
    global logging_client

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info(f"\n*** Running environment {ENVIRONMENT}. ***\n")

    logging_agent = os.getenv("TAP_YFINANCE_LOGGING_AGENT")

    if logging_agent.lower() == "google":
        try:
            from google.cloud import logging as gcp_logging

            logging_client = gcp_logging.Client()
            logging_client.setup_logging()
            logging.info("Configured Google Cloud Logging.")
        except ImportError:
            logging.warning(
                "Google Cloud Logging library not found. Using basic logging."
            )
        except Exception as e:
            logging.error(f"Error setting up Google Cloud Logging: {e}")
    else:
        logging.info(f"Using default basic logging (LOGGING_AGENT: '{logging_agent}').")
    return


def critical_shutdown_handler(signum, frame):
    logging.warning(f"Received signal {signum}. Shutting down...")
    logging_agent = os.getenv("TAP_YFINANCE_LOGGING_AGENT")
    if logging_agent == "google" and logging_client:
        logging.info("Closing Google Cloud Logging client.")
        logging_client.close()
    sys.exit(1)  # Non-zero exit code indicates abnormal shutdown


def get_run_commands(base_run_command, task_chunks, target):
    run_commands = []
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
            f"--state-id tap_yfinance_target_{target}_{ENVIRONMENT}_{state_id} {select_param}".split(
                " "
            )
        )

        run_commands.append(run_command)
    return run_commands


def execute_command(run_command, cwd, concurrency_semaphore=None):
    """Runs a command with optional concurrency semaphore."""
    if concurrency_semaphore:
        with concurrency_semaphore:
            return execute_command_stg(run_command, cwd)
    return execute_command_stg(run_command, cwd)


def execute_command_stg(run_command, cwd):
    """Executes a given command and handles errors."""

    logging.info(f"Running command: {run_command}")

    start = time.monotonic()
    try:
        result = subprocess.run(run_command, cwd=cwd, check=True)
        seconds_taken = time.monotonic() - start
        logging.info(
            f"Command {run_command} completed successfully with return code {result.returncode}. \n"
            f"Subprocess took {round(seconds_taken, 2)} seconds ({round(seconds_taken / 60, 2)} minutes, {round(seconds_taken / 3600, 2)} hours) to succeed."
        )
        return result
    except subprocess.CalledProcessError as e:
        seconds_taken = time.monotonic() - start
        logging.error(
            f"Command {run_command} failed with return code {e.returncode}. \n "
            f"Took {round(seconds_taken, 2)} seconds ({round(seconds_taken / 60, 2)} minutes, {round(seconds_taken / 3600, 2)} hours) to fail."
        )
        logging.error(f"Error output: {e.stderr}")
        return e


def run_meltano_task(
    run_command,
    cwd,
    concurrency_semaphore=None,
    return_queue=None,
):
    """Runs the Meltano task, optionally using concurrency and return_queue if using mp.Process."""

    result = execute_command(
        run_command=run_command, cwd=cwd, concurrency_semaphore=concurrency_semaphore
    )

    if return_queue:
        return_queue.put((run_command, result.returncode))

    return run_command, result.returncode
