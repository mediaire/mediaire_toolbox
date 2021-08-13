import unittest
from unittest.mock import patch
import threading
import time
import logging

# maybe using `fakeredis` would be easier and sufficient?
import redislite as redis
# not needed because we pass redislite instance to RedisSlotWQ via `db=`
# import redislite.patch
# redislite.patch.patch_redis()

from tenacity.retry import retry_if_exception
from tenacity import retry, stop_after_attempt, wait_fixed
from tenacity.before_sleep import before_sleep_log

from mediaire_toolbox.queue import RedisWQ, RedisSlotWQ, QueueDaemon, Task


class MockRedis():
    def __init__(self):
        self.hashmap = {}
        self.expiremap = {}

    def incr(self, key):
        if key in self.hashmap:
            self.hashmap[key] = key + 1
        else:
            self.hashmap[key] = 1
        return self.hashmap[key]

    def get(self, key):
        if key in self.hashmap:
            return self.hashmap[key]
        else:
            return None

    def expire(self, key, time):
        self.expiremap[key] = time

    def execute(self):
        pass

    def pipeline(self):
        return self

    def rpoplpush(self, src, dst):
        value = self.hashmap[src].pop()
        self.hashmap[dst].append(value)
        return value

    def setex(self, *args):
        pass


class TestRedisWQ(unittest.TestCase):
    def setUp(self):
        self.mock_redis = MockRedis()
        self.r_wq = RedisWQ(name='mock_limit_rate', db=self.mock_redis)

    def test_get_limit_key_invalid(self):
        self.assertRaises(ValueError, RedisWQ._get_limit_key, 'invalid')

    def test_limit_get_expiry_time_sec(self):
        self.assertEqual(RedisWQ._get_limit_expirytime('sec'), 1)

    def test_limit_get_expiry_time_min(self):
        self.assertEqual(RedisWQ._get_limit_expirytime('min'), 60)

    def test_limit_get_expiry_time_hour(self):
        self.assertEqual(RedisWQ._get_limit_expirytime('hour'), 60*60)

    def test_limit_get_expiry_time_invalid(self):
        self.assertRaises(ValueError, RedisWQ._get_limit_expirytime, 'invalid')

    def test_limit_rate_no_limit(self):
        # test when no limit, leasing object
        self.assertTrue(self.r_wq._limit_rate(-1, 'hour'))
        self.assertTrue(self.r_wq._limit_rate(-1, 'hour'))

    def test_limit_rate_zero(self):
        with patch.object(RedisWQ, '_get_limit_key', return_value=0):
            self.assertFalse(self.r_wq._limit_rate(0, 'hour'))

    def test_limit_rate(self):
        """Test that the limit rate function limits the rate"""
        with patch.object(RedisWQ, '_get_limit_key', return_value=0):
            # request lease, should return True
            self.assertTrue(self.r_wq._limit_rate(1, 'hour'))
            # same timestamp lease request over limit, should return False
            self.assertFalse(self.r_wq._limit_rate(1, 'hour'))
            self.assertTrue(len(self.mock_redis.expiremap.items()) == 1)
            self.assertEqual(self.mock_redis.expiremap
                             ['mock_limit_rate:limit:0'], 60 * 60)

    def test_limit_rate_reset(self):
        """Test that the limit counter refreshes in the next time bucket"""
        with patch.object(RedisWQ, '_get_limit_key', side_effect=[0, 1]):
            self.assertTrue(self.r_wq._limit_rate(1, 'hour'))
            # different timestamp lease request, return True
            self.assertTrue(self.r_wq._limit_rate(1, 'hour'))
            self.assertTrue(len(self.mock_redis.expiremap.items()) == 2)

    def test_lease(self):
        """Test that the lease returns the item with limit rate"""
        self.mock_redis.hashmap[self.r_wq._main_q_key] = [1, 2]
        self.mock_redis.hashmap[self.r_wq._processing_q_key] = []
        with patch('time.sleep', return_value=None) as mock_sleep, \
            patch.object(RedisWQ, '_get_limit_key', side_effect=[0, 0, 1]), \
                patch.object(RedisWQ, '_itemkey', return_value=""):
            # limit rate function is called three times at these timestamps
            # directly return item
            self.assertEqual(self.r_wq.lease(block=False, limit=1), 2)
            # rate limit process triggered, limit rate function called twice
            self.assertEqual(self.r_wq.lease(block=False, limit=1), 1)
            # sleep function in lease should be called once
            # mock_sleep.assert_called_once()  # TODO python>=3.6
            self.assertEqual(mock_sleep.call_count, 1)

    def test_lease_without_limit(self):
        """Test that the lease returns the item with no limit rate"""
        self.mock_redis.hashmap[self.r_wq._main_q_key] = [1, 2]
        self.mock_redis.hashmap[self.r_wq._processing_q_key] = []
        with patch('time.sleep', return_value=None) as mock_sleep, \
                patch.object(RedisWQ, '_itemkey', lambda *_: ""):
            # directly return item
            self.assertEqual(self.r_wq.lease(block=False, limit=-1), 2)
            # rate limit process triggered, limit rate function called twice
            self.assertEqual(self.r_wq.lease(block=False, limit=-1), 1)
            # sleep function in lease should not be called
            mock_sleep.assert_not_called()


# TODO
# - refactor rate limit with pubsub
# - see what can and should be implemented as redis lua scripts
# - how to do the request timouts (maybe only list tail object)
#
# - remove all explicit redis key names from tests and use class attributes
# - in debug mode register monitor and record messages (in thread)
# - Fix threading hangup
# - pub/sub instead of sleep, wait for `result` key update
# - more multi slot tests
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

DB_FILE = '/tmp/redis.db'


# Run all test cases of `RedisWQ` for `RedisSlotWQ` as well
# TODO why do these patches not work for setUp? patch.TEST_PREFIX?
@patch('mediaire_toolbox.queue.RedisWQ.__init__',
       new=(lambda *args, **kwargs:
            RedisSlotWQ.__init__(*args, **kwargs, slots=1)))
@patch('mediaire_toolbox.queue.RedisWQ', new=RedisSlotWQ)
class TestRedisSlotWQBasic(TestRedisWQ):

    def setUp(self):
        # TODO why does this patch not work?
        # with patch('mediaire_toolbox.queue.RedisWQ', new=RedisSlotWQ):
        super().setUp()
        self.mock_redis = redis.StrictRedis(DB_FILE, decode_responses=True)
        self.mock_redis.flushdb()
        self.r_wq = RedisSlotWQ(name=self.r_wq._main_q_key,
                                db=self.mock_redis,
                                slots=1)

        self.addCleanup(patch.stopall)

        self.mock_redis.expiremap = {}
        # TODO EXPIRETIME is redis>=7.0.0
        # expiremap_patcher = patch.object(
        #     self.mock_redis, 'expiremap',
        #     new_callable=lambda: {key: self.mock_redis.expiretime(key)
        #                           for key in self.mock_redis.keys('*')})
        # expiremap_patcher.start()

        def custom_expire(self_, key, time):
            self.mock_redis.expiremap[key] = time
            # print(self_.__class__.__module__+'.'+self_.__class__.__name__)
            self_.__expire(key, time)
        redis.client.StrictRedis.__expire = redis.client.StrictRedis.expire
        client_expire_patcher = patch('redis.client.StrictRedis.expire',
                                      new=custom_expire)
        client_expire_patcher.start()
        # TODO why does this not work?!
        # blocks `test_limit_rate` and `test_limit_rate_reset`
        # redis.client.Pipeline.__expire = redis.client.StrictRedis.expire
        # pipeline_expire_patcher = patch('redis.client.StrictPipeline.expire',
        #                                new=custom_expire)
        # pipeline_expire_patcher.start()

        self.mock_redis.hashmap = {}
        hashmap_patcher = patch.object(
            self.mock_redis, 'hashmap',
            new_callable=lambda: {key: self.mock_redis.get(key)
                                  for key in self.mock_redis.keys('*')})
        hashmap_patcher.start()

    # TODO
    @unittest.skip("Broken Mock")
    def test_limit_rate(self): pass  # noqa: E704

    # TODO
    @unittest.skip("Broken Mock")
    def test_limit_rate_reset(self): pass  # noqa: E704

    # TODO
    @unittest.skip("Test too implementation specific")
    def test_lease(self): pass  # noqa: E704

    # TODO
    @unittest.skip("Test too implementation specific")
    def test_lease_without_limit(self): pass  # noqa: E704


class TestRedisSlotWQ(unittest.TestCase):

    def setUp(self):
        self.redis = redis.StrictRedis(DB_FILE, decode_responses=True)
        self.redis.flushdb()

    def tearDown(self):
        del self.redis

    def test_config_num_slots(self):
        """Test that the number of slots can be set."""
        num_slots = 23
        test_queue = RedisSlotWQ('slot_test', slots=num_slots, db=self.redis)
        self.assertEqual(len(test_queue._slot_keys), num_slots)

    def test__find_free_slot(self):
        """Test that `_find_free_slot` returns only free slots."""
        num_slots = 2
        test_queue = RedisSlotWQ('slot_test', slots=num_slots, db=self.redis)
        slots = list(test_queue._slot_keys)

        free_slot = test_queue._find_free_slot()
        self.assertTrue(bool(free_slot))

        self.redis.set(slots[0], 1)
        free_slot = test_queue._find_free_slot()
        self.assertTrue(bool(free_slot))

        self.redis.set(slots[1], 1)
        free_slot = test_queue._find_free_slot()
        self.assertFalse(bool(free_slot))

    def test__lock_slot(self):
        """Test that locking a slot works if it is available."""
        test_queue = RedisSlotWQ('slot_test', slots=1, db=self.redis)
        target_slot = list(test_queue._slot_keys)[0]
        test_queue._lock_slot(target_slot, lease_secs=1)
        self.assertEqual(self.redis.get(target_slot), test_queue.sessionID())

    def test__lock_slot_fail(self):
        """Test that locking a slot fails works if it is unavailable."""
        test_queue = RedisSlotWQ('slot_test', slots=1, db=self.redis)
        target_slot = list(test_queue._slot_keys)[0]
        lock_value = 'locked'
        self.redis.set(target_slot, lock_value)

        with self.assertRaises(RuntimeError):
            test_queue._lock_slot(target_slot, lease_secs=1)

        self.assertEqual(self.redis.get(target_slot), lock_value)

    def test__lock_slot_None(self):
        """Test that locking a slot fails if no key is passed."""
        test_queue = RedisSlotWQ('slot_test', slots=1, db=self.redis)
        target_slot = None

        with self.assertRaises(RuntimeError):
            test_queue._lock_slot(None, lease_secs=1)

        self.assertFalse(self.redis.exists(str(target_slot)))

    # TODO
    # def test__lock_slot_expire(self):
    # pubsub to expire events and check time difference

    def test__request_slot(self):
        """Test that enqueuing a slot request works."""
        test_queue = RedisSlotWQ('slot_test', slots=1, db=self.redis)
        test_queue._request_slot(timeout=1)

        # TODO LPOS not implemented yet in redislite==5.0.x
        # self.assertTrue(self.redis.lpos(test_queue._slot_request_list_key,
        #                                 test_queue.sessionID()))
        self.assertIn(test_queue.sessionID(),
                      self.redis.lrange(test_queue._slot_request_list_key,
                                        0, -1))
        self.assertTrue(self.redis.exists(test_queue._slot_request_key))

    # TODO
    # def test__request_slot_expire(self):
    # pubsub to expire events and check time difference

    def test__pop_slot_request(self):
        """Test that `_pop_slot_request` removes requests."""
        test_queue = RedisSlotWQ('slot_test', slots=1, db=self.redis)

        test_queue._request_slot(timeout=1)
        test_queue._pop_slot_request()

        self.assertFalse(self.redis.exists(test_queue._slot_request_list_key))
        self.assertFalse(self.redis.exists(test_queue._slot_request_key))

    def test__new_slot_available_first(self):
        """Test that available slot is acquired if we are first-in-line."""
        test_queue = RedisSlotWQ('slot_test', slots=1, db=self.redis)

        test_queue._request_slot(timeout=1)
        slot = test_queue._new_slot_available(lease_secs=1)

        self.assertTrue(slot)
        self.assertEqual(self.redis.get(slot), test_queue.sessionID())

    def test__new_slot_available_not_first(self):
        """Test that `None` is returned if we are not first-in-line."""
        test_queue = RedisSlotWQ('slot_test', slots=1, db=self.redis)
        other_session_id = 'other_session_id'
        self.assertNotEqual(other_session_id, test_queue.sessionID())
        other_slot_request_key = \
            '{}:{}'.format(test_queue._slot_request_list_key, other_session_id)
        self.redis.set(other_slot_request_key, 'task_id')
        self.redis.lpush(test_queue._slot_request_list_key, other_session_id)

        test_queue._request_slot(timeout=1)
        slot = test_queue._new_slot_available(lease_secs=1)

        self.assertIsNone(slot)
        self.assertEqual(self.redis.lrange(test_queue._slot_request_list_key,
                                           -1, -1)[0],
                         other_session_id)
        self.assertTrue(self.redis.exists(other_slot_request_key))

    def test__new_slot_available_not_first_cleanup(self):
        """Test that expired first-in-line requests are cleaned up."""
        test_queue = RedisSlotWQ('slot_test', slots=1, db=self.redis)
        other_session_id = 'other_session_id'
        self.assertNotEqual(other_session_id, test_queue.sessionID())
        self.redis.lpush(test_queue._slot_request_list_key, other_session_id)

        test_queue._request_slot(timeout=1)
        slot = test_queue._new_slot_available(lease_secs=1)

        self.assertIsNone(slot)
        self.assertNotIn(other_session_id,
                         self.redis.lrange(test_queue._slot_request_list_key,
                                           0, -1))

    # TODO
    @unittest.skip("just for debugging")
    def test__wait_for_slot_pubsub(self):
        test_queue = RedisSlotWQ('slot_test', slots=1, db=self.redis)
        target_slot = list(test_queue._slot_keys)[0]
        lock_value = 'locked'
        self.redis.set(target_slot, lock_value)

        test_queue._wait_for_slot(lease_secs=1, timeout=1)

        self.assertTrue(False)

    def test__wait_for_slot_immediate_acquire(self):
        """Test that a slot is immediately acquired if available."""
        test_queue = RedisSlotWQ('slot_test', slots=1, db=self.redis)

        with patch('mediaire_toolbox.queue.RedisSlotWQ._request_slot') \
                as patched__request_slot:
            slot = test_queue._wait_for_slot(lease_secs=1, timeout=1)

        patched__request_slot.assert_not_called()
        self.assertTrue(slot)
        self.assertTrue(self.redis.exists(slot))

    def test__wait_for_slot_request(self):
        """Check that `_wait_for_slot` calls `_request_slot` if no slot free"""
        test_queue = RedisSlotWQ('slot_test', slots=1, db=self.redis)
        target_slot = list(test_queue._slot_keys)[0]
        lock_value = 'locked'
        self.redis.set(target_slot, lock_value)

        with patch('mediaire_toolbox.queue.RedisSlotWQ._request_slot') \
                as patched__request_slot, \
                patch('redis.client.PubSub.listen', return_value=[]):
            test_queue._wait_for_slot(lease_secs=1, timeout=1)

        patched__request_slot.assert_called_once()
        self.assertEqual(self.redis.get(target_slot), lock_value)

    def test__wait_for_slot_request_acquire_del(self):
        """Check that `_wait_for_slot` acquires slot after `DEL`."""
        test_queue = RedisSlotWQ('slot_test', slots=1, db=self.redis)
        target_slot = list(test_queue._slot_keys)[0]
        other_session_id = 'other_session_id'
        self.redis.set(target_slot, other_session_id)
        self.assertNotEqual(other_session_id, test_queue.sessionID())

        def _wait_for_slot_in_thread():
            acquired_slot = test_queue._wait_for_slot(lease_secs=1, timeout=10)
            self.assertEqual(acquired_slot, target_slot)
        wait_thread = threading.Thread(name="_wait_for_slot_Thread",
                                       target=_wait_for_slot_in_thread)
        wait_thread.start()

        self.assertTrue(self.redis.get(target_slot), other_session_id)
        time.sleep(0.1)

        self.redis.delete(target_slot)
        time.sleep(0.1)
        self.assertEqual(self.redis.get(target_slot), test_queue.sessionID())
        self.assertFalse(wait_thread.is_alive())

    # TODO need versions for DEL, EXPIRED, LREM
    @unittest.skip("not working")
    def test__wait_for_slot_request_acquire_all_methods(self):
        # TODO docstr
        test_queue = RedisSlotWQ('slot_test', slots=1, db=self.redis)
        target_slot = list(test_queue._slot_keys)[0]
        other_session_id = 'other_session_id'
        self.assertNotEqual(other_session_id, test_queue.sessionID())

        def _wait_for_slot_in_thread():
            acquired_slot = test_queue._wait_for_slot(lease_secs=1, timeout=10)
            self.assertEqual(acquired_slot, target_slot)

        change_funcs = [(self.redis.delete, (target_slot)),
                        (self.redis.expire, (target_slot, 0)),
                        (self.redis.lrem, (test_queue._slot_request_list_key,
                                           0,
                                           other_session_id))]
        for change_func, change_args in change_funcs:
            # TODO why is subtest not working?
            with self.subTest(method=change_func.__name__):
                print(change_func.__name__)
                self.tearDown()
                self.setUp()  # clear database

                self.redis.set(target_slot, other_session_id)
                self.redis.lpush(test_queue._slot_request_list_key,
                                 other_session_id)

                wait_thread = threading.Thread(name="_wait_for_slot_Thread",
                                               target=_wait_for_slot_in_thread)
                wait_thread.start()

                self.assertTrue(self.redis.get(target_slot), other_session_id)
                time.sleep(0.1)

                change_func(*change_args)
                time.sleep(0.1)
                self.assertEqual(self.redis.get(target_slot),
                                 test_queue.sessionID())
                self.assertFalse(wait_thread.is_alive())

    # TODO
    # def test__wait_for_slot_locked_in_between(self):


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


@unittest.skip("Not relevant atm")
class TestRedisSlotWQDaemon(unittest.TestCase):
    INPUT = 'input'
    RESULT = 'result'

    def setUp(self):
        self.redis = redis.StrictRedis(DB_FILE, decode_responses=True)

        self.input_queue = RedisSlotWQ(self.INPUT, slots=2, db=self.redis)
        self.result_queue = RedisSlotWQ(self.RESULT, slots=2, db=self.redis)

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

    def test_die_during_execution_max_retries(self):
        """Test crashing Task is put into error queue after max retries."""
        self._queue_tasks([Task(t_id=0)])
        self._add_daemon(CrashRetryDaemon, lease_secs=10)
        self._start_daemons()

        time.sleep(RETRY_FORCED_CRASH_TIMES * RETRY_FORCED_CRASH_SECONDS + 1)

        self.assertEqual(self.redis.llen(self.INPUT + ':errors'), 1)
