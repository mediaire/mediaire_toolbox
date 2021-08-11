# TODO
# - refactor rate limit with pubsub
# - bypass that skips on -1
# - see what can and should be implemented as redis lua scripts
# - how to do the request timouts (maybe only list tail object)
#
# - maybe split test suite into single method tests and queue daemon tests
# - Fix threading hangup
# - pub/sub instead of sleep, wait for `result` key update
# - incorporate into test_redis_wg.py
# - tests
#     - immediate acquire
#     - enqueue to waiting list
#     - die during execution
#     - die during exectuion retry
#     - die during waiting
#     - die during waiting retry
#     - hangup during exectuion
#     - hangup during exectuion retry
#     - hangup during waiting
#     - hangup during waiting retry
#     - first to request
#     - last to request
#     - ambigous t_id
#     - invalid timeout/lease_time
#     - rate limiting
#         config={'lease_limit': 1,
#                 'limit_timeunit': 'sec'})

import unittest
from unittest.mock import patch
import threading
import time
import logging

# maybe using `fakeredis` would be easier and sufficient?
import redislite as redis
# not needed because we pass redislite instance to RedisWQ via `db=`
# import redislite.patch
# redislite.patch.patch_redis()

from tenacity.retry import retry_if_exception
from tenacity import retry, stop_after_attempt, wait_fixed
from tenacity.before_sleep import before_sleep_log

from mediaire_toolbox.queue import RedisWQ, QueueDaemon, Task


default_logger = logging.getLogger(__name__)
RETRY_FORCED_CRASH_TIMES = 5
RETRY_FORCED_CRASH_SECONDS = 1


def forced_crash_retry(f):
    """Emulates behavior of `ml_commons.segmentation.utils.cuda_retry`."""
    return retry(
        retry=(retry_if_exception(lambda e: (isinstance(e, RuntimeError)
                                             and 'Forced Crash' in str(e)))),
        stop=stop_after_attempt(RETRY_FORCED_CRASH_TIMES),
        wait=wait_fixed(RETRY_FORCED_CRASH_SECONDS),
        before_sleep=before_sleep_log(default_logger, logging.INFO))(f)


class PrintDaemon(QueueDaemon):
    """Just prints task metadata."""
    def process_task(self, task):
        # TODO use logging
        print('[{}]'.format(self.daemon_name),
              task.t_id,
              'timestamp:', task.timestamp,
              'update_timestamp:', task.update_timestamp)


class SleepDaemon(QueueDaemon):
    """Sleeps for `config['sleep']` seconds before compleeting."""
    def process_task(self, task):
        sleep = self.config.get('sleep', 0)
        # TODO use logging
        print('[{}]'.format(self.daemon_name),
              'sleep', sleep,
              task.t_id,
              'timestamp:', task.timestamp,
              'update_timestamp:', task.update_timestamp)
        time.sleep(sleep)
        print('[{}]'.format(self.daemon_name),
              'done',
              task.t_id,
              'timestamp:', task.timestamp,
              'update_timestamp:', task.update_timestamp)


class CrashDaemon(QueueDaemon):
    """Raises `RuntimeError('Forced Crash')`."""
    def process_task(self, task):
        # TODO use logging
        print('[{}]'.format(self.daemon_name),
              task.t_id,
              'timestamp:', task.timestamp,
              'update_timestamp:', task.update_timestamp)
        raise RuntimeError('Forced Crash')


class CrashRetryDaemon(CrashDaemon):
    """Raises `RuntimeError('Forced Crash')` and retries using decorator."""
    @forced_crash_retry
    def process_task(self, task):
        super().process_task(task)


class HangupDaemon(QueueDaemon):
    """Hangs up in an endless loop."""
    def process_task(self, task):
        # TODO use logging
        print('[{}]'.format(self.daemon_name),
              task.t_id,
              'timestamp:', task.timestamp,
              'update_timestamp:', task.update_timestamp)
        while True:
            pass


class TestResourcManager(unittest.TestCase):
    DB_FILE = '/tmp/redis.db'
    INPUT = 'input'
    RESULT = 'result'

    def setUp(self):
        self.redis = redis.StrictRedis(self.DB_FILE, decode_responses=True)

        self.input_queue = RedisWQ(self.INPUT, db=self.redis)
        self.result_queue = RedisWQ(self.RESULT, db=self.redis)

        self.daemons = []

        self.redis.flushdb()

    def tearDown(self):
        for daemon, _ in self.daemons:
            daemon.stop()
            daemon.exit_gracefully(_, _)

        del self.redis

    def _queue_tasks(self, tasks):
        for task in tasks:
            self.input_queue.put(task.to_bytes())

    def _start_daemons(self):
        for _, thread in self.daemons:
            thread.start()

    # TODO for some reason, QueueDaemon.exit_gracefully is not working
    # correctly and to interrupt the program, so prevent it trapping SIGINT
    @patch('signal.signal')
    def _add_daemon(self, DaemonClass, _, lease_secs=1, config={}):
        name = "{}-{}".format(DaemonClass.__name__, len(self.daemons))
        daemon = DaemonClass(daemon_name=name,
                             input_queue=self.input_queue,
                             result_queue=self.result_queue,
                             lease_secs=lease_secs,
                             config=config)
        daemon_thread = threading.Thread(name=name + 'Thread',
                                         target=daemon.run)
        self.daemons.append((daemon, daemon_thread))

    def test_config_num_slots(self):
        """Test that the number of GPU slots can be set."""
        num_slots = 23
        test_queue = RedisWQ('slot_test', db=self.redis, gpu_slots=num_slots)
        self.assertEqual(len(test_queue._slot_keys), num_slots)

    def test__find_free_slot(self):
        """Test that `_find_free_slot` returns only free slots."""
        num_slots = 2
        test_queue = RedisWQ('slot_test', db=self.redis, gpu_slots=num_slots)

        free_slot = test_queue._find_free_slot()
        self.assertTrue(bool(free_slot))

        self.redis.set('slot:0', 1)
        free_slot = test_queue._find_free_slot()
        self.assertTrue(bool(free_slot))

        self.redis.set('slot:1', 1)
        free_slot = test_queue._find_free_slot()
        self.assertFalse(bool(free_slot))

    def test__lock_slot(self):
        """Test that locking a slot works if it is available."""
        target_slot = 'slot:0'
        test_queue = RedisWQ('slot_test', db=self.redis, gpu_slots=1)
        test_queue._lock_slot(target_slot, lease_secs=1)
        self.assertEqual(self.redis.get(target_slot), test_queue.sessionID())

    def test__lock_slot_fail(self):
        """Test that locking a slot fails works if it is unavailable."""
        target_slot = 'slot:0'
        lock_value = 'locked'
        self.redis.set(target_slot, lock_value)

        test_queue = RedisWQ('slot_test', db=self.redis, gpu_slots=1)
        with self.assertRaises(RuntimeError):
            test_queue._lock_slot(target_slot, lease_secs=1)

        self.assertEqual(self.redis.get(target_slot), lock_value)

    # TODO
    # def test__lock_slot_expire(self):
    # pubsub to expire events and check time difference

    def test__request_slot(self):
        """Test that enqueuing a slot request works."""
        test_queue = RedisWQ('slot_test', db=self.redis, gpu_slots=1)
        test_queue._request_slot(timeout=1)

        # TODO LPOS not implemented yet
        # self.assertTrue(self.redis.lpos(test_queue._slot_request_key,
        #                                 test_queue.sessionID()))
        self.assertIn(test_queue.sessionID(),
                      self.redis.lrange(test_queue._slot_request_key, 0, -1))
        self.assertTrue(self.redis.exists(test_queue._slot_request_key + ':'
                                          + test_queue.sessionID()))

    # TODO
    # def test__request_slot_expire(self):
    # pubsub to expire events and check time difference

    # TODO
    # def test_slotting_disabled_bypass(self):
    # """Check that passing `gpu_slots=-1` bypasses all slotting."""
    # patch all _slot methods and check they are not called

    def test_die_during_execution_max_retries(self):
        """Test crashing Task is put into error queue after max retries."""
        self._queue_tasks([Task(t_id=0)])
        self._add_daemon(CrashRetryDaemon, lease_secs=10)
        self._start_daemons()

        time.sleep(RETRY_FORCED_CRASH_TIMES * RETRY_FORCED_CRASH_SECONDS + 1)

        self.assertEqual(self.redis.llen(self.INPUT + ':errors'), 1)
