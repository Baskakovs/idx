"""idx."""

import logging

from prefect.exceptions import MissingContextError
from prefect.logging import get_run_logger


def get_logger(name: str = __name__) -> logging.Logger:
    """Get a Prefect run logger if inside a flow/task, otherwise a standard logger."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(name)
