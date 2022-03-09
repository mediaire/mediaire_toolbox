import logging

from tenacity.retry import retry_if_exception
from tenacity import retry, stop_after_attempt, wait_fixed


from mediaire_toolbox.constants import (
    RETRY_DATABASE_OP_SECONDS,
    RETRY_DATABASE_OP_TIMES
)

default_logger = logging.getLogger(__name__)


"""
Database retry logic for the Transactions DB.
Retry up to a maximum amount of times, with a fixed wait period inbetween.
Retry only for certain exceptions that we know are problematic.
"""


def before_sleep_log(retry_state):
    default_logger.warn(
        'Retrying {}: attempt {} ended with: {}'
        .format(retry_state.fn, retry_state.attempt_number,
                retry_state.outcome))


def is_operational_error(e):
    return (
        'OperationalError' in str(e) or
        'OperationalError' in e.__class__.__name__
    )


def t_db_retry(f):
    return retry(
        retry=(
            retry_if_exception(is_operational_error)),
        stop=stop_after_attempt(RETRY_DATABASE_OP_TIMES),
        wait=wait_fixed(RETRY_DATABASE_OP_SECONDS),
        before_sleep=before_sleep_log)(f)
