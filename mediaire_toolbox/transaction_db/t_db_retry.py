import logging

from tenacity.retry import (retry_if_exception_type,
                            retry_if_not_exception_message)
from tenacity import retry, stop_after_attempt, wait_fixed

from sqlite3 import OperationalError as Sqlite3OperationalError
from sqlalchemy.exc import OperationalError

from mediaire_toolbox.constants import (
    RETRY_DATABASE_OP_SECONDS,
    RETRY_DATABASE_OP_TIMES
)

default_logger = logging.getLogger(__name__)


"""
Database retry logic for the Transactions DB.
Retry up to a maximum amount of times, with a fixed wait period inbetween.
Retry only for certain exceptions that we know are problematic.
Don't retry when attempting write operations on read-only databases.
"""


def before_sleep_log(retry_state):
    default_logger.warning(
        'Retrying {}: attempt {} ended with: {}'
        .format(retry_state.fn, retry_state.attempt_number,
                retry_state.outcome))


def t_db_retry(f):
    """Retries only when specific sqlite3 Exceptions are raised and when
    they don't correspond failed write operations on read-only databases
    (message will be something like 'attempt to write a readonly database')
    """
    return retry(
        retry=(
            (retry_if_exception_type(OperationalError)
             | retry_if_exception_type(Sqlite3OperationalError))
            & retry_if_not_exception_message(match='.*read.?only.*')
        ),
        stop=stop_after_attempt(RETRY_DATABASE_OP_TIMES),
        wait=wait_fixed(RETRY_DATABASE_OP_SECONDS),
        before_sleep=before_sleep_log)(f)
