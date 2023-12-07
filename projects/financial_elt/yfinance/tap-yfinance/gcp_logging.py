import sys
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler

import os

PROJECT_ID = os.getenv('GCP_PROJECT_ID')

def get_handler(*, project=PROJECT_ID, logger_name='meltano'):
    """ Return a handler based on the environment. """
    client = google.cloud.logging.Client(project=project)
    return CloudLoggingHandler(client, name=logger_name)
