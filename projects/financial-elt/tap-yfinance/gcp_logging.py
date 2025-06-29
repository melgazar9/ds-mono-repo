import logging
import os
import sys


def get_handler(*, project=os.getenv("GCP_PROJECT_ID"), logger_name=None):
    """Return a handler based on the environment."""
    try:
        import google.cloud.logging
        from google.cloud.logging.handlers import CloudLoggingHandler

        client = google.cloud.logging.Client(project=project)
        handler = CloudLoggingHandler(client, name=logger_name, batch_size=32000, batch_timeout=120)

        handler.setFormatter(logging.Formatter("%(message)s"))
        return handler
    except Exception as e:
        print(f"Failed to create logging client: {e}", file=sys.stderr)
        raise
