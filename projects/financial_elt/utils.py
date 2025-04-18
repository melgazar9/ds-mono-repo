from flask import Flask, make_response, request, redirect, Blueprint, render_template
import os
import subprocess
from datetime import datetime
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import yaml
import multiprocessing as mp


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


def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info(f"\n*** Running environment {ENVIRONMENT}. ***\n")

    logging_agent = os.getenv("LOGGING_AGENT")

    if logging_agent.lower() == "google":
        try:
            from google.cloud import logging as gcp_logging
            global logging_client
            logging_client = gcp_logging.Client()
            logging_client.setup_logging()
            logging.info("Configured Google Cloud Logging.")
        except ImportError:
            logging.warning("Google Cloud Logging library not found. Using basic logging.")
        except Exception as e:
            logging.error(f"Error setting up Google Cloud Logging: {e}")
    else:
        logging.info(f"Using default basic logging (LOGGING_AGENT: '{logging_agent}').")
    return

def shutdown_handler(signum, frame):
    logging.info(f"Received signal {signum}. Shutting down...")
    logging_agent = os.getenv("LOGGING_AGENT")
    if logging_agent == "google" and logging_client:
        logging.info("Closing Google Cloud Logging client.")
        logging_client.close()
    exit(0)

def run_meltano_task(run_command, concurrency_semaphore, cwd):
    setup_logging()

    with concurrency_semaphore:
        logging.info(f"Running command: {run_command}")
        subprocess.run(run_command, cwd=cwd)