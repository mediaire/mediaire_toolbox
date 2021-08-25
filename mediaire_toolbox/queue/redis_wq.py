"""Work queue based on redis.

References
----------
Adapted from
    https://kubernetes.io/docs/tasks/job/fine-parallel-processing-work-queue/
    https://kubernetes.io/examples/application/job/redis/rediswq.py
"""

from typing import Optional
import uuid
import hashlib
import logging
import time

import redis


logger = logging.getLogger(__name__)


class RedisWQ(object):
    """Simple Finite Work Queue with Redis Backend.

    This work queue is finite: as long as no more work is added
    after workers start, the workers can detect when the queue
    is completely empty.

    The items in the work queue are assumed to have unique values.

    This object is not intended to be used by multiple threads
    concurrently.
    """
    def __init__(self,
                 name: str,
                 db: Optional[redis.client.Redis] = None,
                 **redis_kwargs):
        """
        Parameters
        ----------
        name
            The work queue is identified by `name`. The library may create
            other keys with `name` as a prefix.
        db
            A custom redis database object.
        **redis_kwargs
            Additional parameters that will be passed to the
            `redis.StrictRedis` constructor of the database object. The default
            connection parameters are:
                host='localhost', port=6379, db=0,
        """
        if db is None:
            self._db = redis.StrictRedis(**redis_kwargs)
        else:
            self._db = db
        # The session ID will uniquely identify this "worker".
        self._session = str(uuid.uuid4())
        # Work queue is implemented as two queues: main, and processing.
        # Work is initially in main, and moved to processing when a client
        # picks it up.
        self._main_q_key = name
        self._processing_q_key = name + ":processing"
        self._error_q_key = name + ":errors"
        self._error_messages_q_key = name + ":error_messages"
        self._lease_key_prefix = name + ":leased_by_session:"
        self._limit_key_prefix = name + ":limit:"

    def sessionID(self):
        """Return the ID for this session."""
        return self._session

    def _main_qsize(self):
        """Return the size of the main queue."""
        return self._db.llen(self._main_q_key)

    def _processing_qsize(self):
        """Return the size of the processing queue."""
        return self._db.llen(self._processing_q_key)

    def empty(self):
        """Return True if the queue is empty, including work being done, False
        otherwise.

        False does not necessarily mean that there is work available to work
         on right now,
        """
        return self._main_qsize() == 0 and self._processing_qsize() == 0

    # TODO: implement this
    # def check_expired_leases(self):
    #     """Return to the work queueReturn True if the queue is empty,
    #     False otherwise."""
    #     # Processing list should not be _too_ long since it is approximately
    #     # as long
    #     # as the number of active and recently active workers.
    #     processing = self._db.lrange(self._processing_q_key, 0, -1)
    #     for item in processing:
    #       # If the lease key is not present for an item (it expired or was
    #       # never created because the client crashed before creating it)
    #       # then move the item back to the main queue so others can work on
    #       # it.
    #       if not self._lease_exists(item):
    #           # TODO: transactionally move the key from processing queue to
    #           # to main queue, while detecting if a new lease is created
    #           # or if either queue is modified.

    def _itemkey(self, item):
        """Returns a string that uniquely identifies an item (bytes)."""
        return hashlib.sha224(item).hexdigest()

    def _lease_exists(self, item):
        """True if a lease on 'item' exists."""
        return self._db.exists(self._lease_key_prefix + self._itemkey(item))

    def put(self, item):
        self._db.lpush(self._main_q_key, item)

    @staticmethod
    def _get_limit_key(timeunit):
        if timeunit == 'sec':
            return time.gmtime(time.time()).tm_sec
        if timeunit == 'min':
            return time.gmtime(time.time()).tm_min
        if timeunit == 'hour':
            return time.gmtime(time.time()).tm_hour
        raise ValueError('Invalid timeunit {}'.format(timeunit))

    @staticmethod
    def _get_limit_expirytime(timeunit):
        """
        Returns the expirytime of a key of the rate limiter
        The rate limited if the leases of a timeunit reaches its limit.
        That is also why an expirytime of the counter must be set in order
        for the counter to be reset to 0 in the next cycle.

        Returns
        -------
        int
            seconds of lifetime for each bucket
        """
        if timeunit == 'sec':
            return 1
        if timeunit == 'min':
            return 60
        if timeunit == 'hour':
            return 60 * 60
        raise ValueError('Invalid timeunit {}'.format(timeunit))

    def _limit_rate(self, limit, timeunit):
        """Limits the rate of items leased in the queue.

        Counts the leases at each timeunit, after the limit is reached, no more
        leases are allowed.

        Parameters
        ----------
        limit: int
            Maximum leases per timeunit, Negative if there is no limit.
        timeunit: str
            Timeunit of the rate limiter. Either 'sec', 'min' or 'hour'

        Returns
        -------
        bool
            True if limit not reached, False if lease hit maximum rate.
        """
        if limit < 0:
            return True
        key = self._get_limit_key(timeunit)
        rate_key = self._limit_key_prefix + str(key)
        result = self._db.get(rate_key)
        result = int(result) if result else 0
        if result >= limit:
            return False
        expiry_time = self._get_limit_expirytime(timeunit)
        # atomic operation
        pipe = self._db.pipeline()
        pipe.incr(rate_key)
        pipe.expire(rate_key, expiry_time)
        pipe.execute()
        return True

    # for now `lease_secs` is useless!
    def lease(self,
              lease_secs: int = 5,
              block: bool = True,
              timeout: Optional[int] = None,
              limit: int = -1,
              timeunit: str = 'hour'):
        """Begin working on an item the work queue.

        Check if rate reached limit on work queue. If reached, wait until next
        timeunit.  If not, then lease item.

        Parameters
        ----------
        lease_secs
            Lease the item for lease_secs.  After that time, other
            workers may consider this client to have crashed or stalled
            and pick up the item instead.
        block
            True if block until an item is available.
        timeout
            Timeout for blocking until an item is available. None to wait
            unlimited time.
        limit
            Maximum leases per timeunit, Negative if there is no limit.
        timeunit
            Timeunit of the rate limiter. Either 'sec', 'min' or 'hour'

        Returns
        -------
        bytes
            Leased item in bytes.
        """

        # first try to get an item from the queue (if block: wait for an item)
        if block:
            # TODO redis>=6.2.0: use BLMOVE {} {} RIGHT LEFT
            item = self._db.brpoplpush(self._main_q_key,
                                       self._processing_q_key,
                                       timeout=timeout)
        else:
            # TODO redis>=6.2.0: use LMOVE {} {} RIGHT LEFT
            item = self._db.rpoplpush(self._main_q_key, self._processing_q_key)

        if item:
            # Check if rate limit for this queue type has been exceeded. If so,
            # wait until next epoch.
            # NOTE: if we crash at this line of the program, then GC will
            # see no lease for this item a later return it to the main queue.
            logger.info('Leasing item from queue {} with Limit {} per {}'
                        .format(self._main_q_key, limit, timeunit))
            # TODO this could probably be solved more elegantly with pubsub
            while True:
                if self._limit_rate(limit, timeunit):
                    break
                sleeptime = 1.0
                time.sleep(sleeptime)

            # Record that we (this session id) are working on a key. Expire
            # that note after the lease timeout.
            itemkey = self._itemkey(item)
            logger.info('{} -> {}'.format(self._lease_key_prefix + itemkey,
                                          self._session))
            self._db.setex(self._lease_key_prefix + itemkey,
                           lease_secs,
                           self._session)
        return item

    def error(self, value, msg=None):
        """Handle the case when processing of the item with 'value' failed.

        Optionally provide error message `msg`.
        """
        itemkey = self._itemkey(value)
        if msg is None:
            msg = 'unknown error'
        logger.info("{}: Trying to move '{}' to '{}'".format(msg, itemkey,
                                                             self._error_q_key)
                    )
        exit_code = self._db.lrem(self._processing_q_key, 0, value)
        logger.debug("exit code: {}".format(exit_code))
        if exit_code == 0:
            logger.error("Could not find '{}' in '{}'".format(
                itemkey, self._processing_q_key))
        else:
            len_errors = self._db.lpush(self._error_q_key, value)
            len_msgs = self._db.lpush(self._error_messages_q_key,
                                      msg.encode('utf-8'))
            logger.debug("Moved '{}' to '{}'".format(itemkey,
                                                     self._error_q_key))
            assert len_errors == len_msgs

    def complete(self, value):
        """Complete working on the item with 'value'.

        If the lease expired, the item may not have completed, and some
        other worker may have picked it up.  There is no indication
        of what happened.
        """
        self._db.lrem(self._processing_q_key, 0, value)
        # If we crash here, then the GC code will try to move the value,
        # but it will
        # not be here, which is fine.  So this does not need to be a
        # transaction.
        itemkey = self._itemkey(value)
        self._db.delete(self._lease_key_prefix + itemkey, self._session)

    @staticmethod
    def get_all_queues_from_config(appconfig: dict, redis_args: dict):
        queues = {
            q_identifier: RedisWQ(q_key, **redis_args)
            for q_identifier, q_key in appconfig['shared']['queues'].items()
        }
        return queues


# TODO: add functions to clean up all keys associated with "name" when
# processing is complete.

# TODO(etune): finish code to GC expired leases, and call periodically
#  e.g. each time lease times out.
# edit: each leased item generates a leased_key dicom_folders:leased_
# by_session:item_key with the value of the session. One could periodically
# check if all items in the processing q have a leased_key. If not, these are
# expired leases and should be put back into the main q


class RedisSlotWQ(RedisWQ):
    def __init__(self,
                 name: str,
                 slots: int,
                 db: Optional[redis.client.Redis] = None,
                 **redis_kwargs):
        """
        Parameters
        ----------
        name
            The work queue is identified by `name`. The library may create
            other keys with `name` as a prefix.
        slots
            Number of slots that can be used by workers in vertical scaling
            mode. Basically the number of workers that are allowed to use
            resources in parallel (which is not necessarily the number of
            workers allowed to use the CPU in parallel,
            `envconfig.mdbrain_scale`).
        db
            A custom redis database object. `decode_responses=True` _must_ be
            set. If None is passed, a new redis connection will be created with
            **redis_kwargs.
        **redis_kwargs
            Additional parameters that will be passed to the
            `redis.StrictRedis` constructor of the database object. The default
            connection parameters are:
                host='localhost', port=6379, db=0,
            Because the implementation assumes that `decode_responses=True` is
            set, this value will always override what has been set in
            **redis_kwargs
        """
        if db is None:
            # always set `decode_responses=True`, override `redis_kwargs`
            db = redis.StrictRedis(**{**redis_kwargs,
                                      **{'decode_responses': True}})

        super().__init__(name=name, db=db, **redis_kwargs)

        # we need to enable keyspace events for string keys to listen for
        # slot lock releases
        # K: Keyspace events, published with __keyspace@<db>__ prefix
        # g: Generic commands (non-type specific) like DEL, EXPIRE, RENAME, ...
        # l: List commands
        # x: Expired events (events generated every time a key expires)
        self._db.config_set('notify-keyspace-events', 'Kglx')

        # TODO
        # envconfig = md_commons.utils.read_yaml(md_commons.constants.ENVCONFIG_PATH)  # noqa: 501
        # Vertical scaling is done by setting
        # - `max_processing_studies`: number of Tasks processed in parallel
        # - `mdbrain_scale`: number of worker images spun up
        # but unfortunately, RedisWQ is imported by md_commons, so this would
        # lead to a circular import. For now, use manual parameter.
        self._slots = slots
        # NOTE slot keys are _not_ unique per session but shared across all
        # sessions!
        self._slot_key_prefix = 'slot'
        self._slot_request_list_key = 'slot_request'
        self._slot_request_key = '{}:{}'.format(self._slot_request_list_key,
                                                self.sessionID())
        if self._slots > 0:
            self._slot_keys = set('{}:{}'.format(self._slot_key_prefix, slot)
                                  for slot in range(self._slots))

    @staticmethod
    def _px(ex: float):
        """Convert EX in fractional seconds to PX in integer miliseconds."""
        return int(ex * 1000)

    def _find_free_slot(self):
        """Return key to free resource slot if available, None otherwise."""
        blocked_slots = set(self._db.keys('{}:*'
                                          .format(self._slot_key_prefix)))
        free_slots = self._slot_keys - blocked_slots
        try:
            return free_slots.pop()
        except KeyError:
            return None

    def _lock_slot(self, slot_key: Optional[str], lease_secs: int):
        """Lock slot `slot_key` to current session.

        A slot taken if its redis key from :var:`self._slot_keys` that is set
        to the curr

        Parameters
        ----------
        slot_key
            redis key for the slot to lock
        lease_secs
            Number of seconds to lock the slot for. This should be the maximum
            time a worker is allowed to take to process a Task before we assume
            it died or hung up and we release the slot.

        Raises
        ------
        RuntimeError
            If unable to lock slot (slot might not exist or not be available).
        """
        done = (self._db.set(slot_key, self.sessionID(),
                             nx=True, px=self._px(lease_secs))
                if slot_key else False)
        if not done:
            raise RuntimeError("Slot `{}` could not be locked."
                               .format(slot_key))

    # TODO (configurable) default timeout? How to choose timeouts?
    # maybe timeouts should be reset every time the queue gets popped?
    # or only for the first item?
    def _request_slot(self, timeout: int, task_id: str = ''):
        """Enqueue a request to be eligible when a slot becomes available.

        The request queue is a redis list. New requests are pushed in from the
        left (head) in the form of session IDs. The right-most ID is allowed to
        take a slot and pop itself off the request list. See
        :meth:`_remove_slot_request()`.

        To prevent a deadlock if a worker dies during waiting, an additional
        key of the form `slot_request:{sessionID} -> {task_id}` is set with
        with a `timeout`. When workers check the tail of the request, they
        clean up expired requests. See :meth:`_cleanup_slot_requests()`

        Parameters
        ----------
        timeout
            The number of seconds after which the request should be discarded.
            If the worker dies while waiting, the queue doesn't get locked up.
        task_id
            Value identifying the task the slot is reserved for. This usually
            is :meth:`._itemkey()` of the `item` to be processed. Can be empty.
        """
        self._db.lpush(self._slot_request_list_key, self.sessionID())
        # TODO should we do nx=True here and handle errors?
        self._db.set(self._slot_request_key, task_id, px=self._px(timeout))

    # TODO
    # self._slot_request_list_key -> self._slot_request_list_key
    # @property
    # def _slot_request_list_key:
    #     return self._slot_request_list_key + ':' + self.sessionID()

    # TODO detailed docs, test
    def _pop_slot_request(self):
        """Remove slot request from the request queue."""
        removed_session = self._db.rpop(self._slot_request_list_key)
        assert removed_session == self.sessionID()
        self._db.delete(self._slot_request_key)

    def _new_slot_available(self, lease_secs: float) -> Optional[str]:
        # TODO more detailed docs, test
        """When a new slot available, acquire it if eligible."""
        # TODO check if we expired already + test
        # TODO make this atomic with transaction, probably needs to be lua
        # instead of redis.pipeline, we can use
        # self.redis.execute('MULTI')
        # self._perform_action()
        # self.redis.execute('EXEC')
        lrange = self._db.lrange(self._slot_request_list_key, -1, -1)
        first_in_line = lrange[0] if lrange else None
        if first_in_line == self.sessionID():
            slot_key = self._find_free_slot()
            self._lock_slot(slot_key, lease_secs=lease_secs)
            self._pop_slot_request()
            return slot_key
        elif not self._db.exists("{}:{}".format(self._slot_request_list_key,
                                                first_in_line)):
            # The current worker first-in-line's slot request has expired, so
            # they won't pick their slot up. Remove them from the request
            # queue. This will publish an event to waiting workers (including
            # this one), so they right will be notified.
            # We are _not_ using RPOP here because all waiting clients will
            # perform this cleanup action. With M workers `LLEN slot_request`
            # can not be more than M. If all M requests have expired and all
            # workers check the whole queue in the most pathological order
            # (the one who is first in line checks last), the whole queue will
            # still be cleared out.
            self._db.lrem(self._slot_request_list_key, 0, first_in_line)
        # TODO reset expire on new front item + test
        return None

    def _wait_for_slot(self, lease_secs: float, timeout: float) -> str:
        # TODO docs: review timeout param, still correct?
        """Wait until a GPU slot is available, and lock it.

        If no slot is available immediately, enqueue in request queue, wait
        for slot freeing and clean up expired request entries.

        This uses redis PubSub to wait blockingly instead of polling.

        Parameters
        ----------
        lease_secs
            Lock the slot for `lease_secs` seconds. After that time, other
            workers may consider this client to have crashed or stalled
            and pick up the item instead.
        timeout
            Number of seconds after which trying to acquire a slot will be
            given up.

        Returns
        -------
        str
            redis key of the acquired slot
        """
        slot = None
        try:
            slot = self._find_free_slot()
            logger.debug("Got slot {}".format(slot))
            self._lock_slot(slot, lease_secs=lease_secs)
            logger.debug("Locked slot {}".format(slot))
            return slot
        except RuntimeError:
            # Slot was locked by somebody else between `_find_free_slot()` and
            # `_lock_slot()` call.
            # Or: `slot` was `None`, no free slot available in the first place.
            logger.debug("Could not lock slot {}".format(slot))

            # enqueue in request line
            self._request_slot(timeout=timeout)
            logger.debug("Requested slot {}".format(slot))

            # TODO what if I'm the last task and the expire event fires after
            # find_free_slot but before psubscribe?

            # wait for slots to get free or the slot request queue to be
            # changed
            # TODO maybe use pipe.watch?
            pubsub = self._db.pubsub()
            pubsub.psubscribe('__keyspace@*:{}:*'
                              .format(self._slot_key_prefix))
            pubsub.psubscribe('__keyspace@*:{}'
                              .format(self._slot_request_list_key))
            # TODO need to subscribe to list change events as well!
            logger.debug("Subscribed")
            for event in pubsub.listen():
                logger.debug("Received event: {}".format(event))
                if event['type'] != 'pmessage':
                    continue
                if (event['data'] == 'del'
                        or event['data'] == 'expired'
                        or event['data'] == 'lrem'):
                    free_slot = ':'.join(event['channel'].split(':')[1:])
                    logger.debug("Newly available slot: {}".format(free_slot))
                    slot = self._new_slot_available(lease_secs)
                    logger.debug("Processed {}, got slot: {}"
                                 .format(self._slot_request_list_key, slot))
                    if slot:
                        logger.debug("returning {}".format(slot))
                        return slot
                    # If we could not acquire a slot, just continue listening
                    # for more events.

    def lease(self,
              lease_secs: float,
              block: bool = True,
              timeout: Optional[float] = None,
              limit: int = -1,
              timeunit: str = 'hour'):
        # TODO docs: review timeout param, still correct? None still possible?
        """Begin working on an item the work queue.

        Check if rate reached limit on work queue. If reached, wait until next
        timeunit.  If not, then lease item.

        Parameters
        ----------
        lease_secs
            Lease the item for `lease_secs` seconds. After that time, other
            workers may consider this client to have crashed or stalled
            and pick up the item instead. Fractional values are supported.
        block
            True if block until an item is available.
        timeout
            Timeout for blocking until an item is available. None to wait
            unlimited time.
        limit
            Maximum leases per timeunit, Negative if there is no limit.
        timeunit
            Timeunit of the rate limiter. Either 'sec', 'min' or 'hour'

        Returns
        -------
        bytes
            Leased item in bytes.
        """

        # first try to get an item from the queue (if block: wait for an item)
        if block:
            # TODO redis>=6.2.0: use BLMOVE {} {} RIGHT LEFT
            item = self._db.brpoplpush(self._main_q_key,
                                       self._processing_q_key,
                                       timeout=timeout).encode('utf-8')
        else:
            # TODO redis>=6.2.0: use LMOVE {} {} RIGHT LEFT
            item = self._db.rpoplpush(self._main_q_key, self._processing_q_key)

        if item:
            # Check if rate limit for this queue type has been exceeded. If so,
            # wait until next epoch.
            # NOTE: if we crash at this line of the program, then GC will
            # see no lease for this item a later return it to the main queue.
            logger.info('Leasing item from queue {} with Limit {} per {}'
                        .format(self._main_q_key, limit, timeunit))
            # TODO this could probably be solved more elegantly with pubsub
            while True:
                if self._limit_rate(limit, timeunit):
                    break
                sleeptime = 1.0
                time.sleep(sleeptime)

            # TODO change comment
            # Only lease the item if a slot is available, otherwise secure
            # a spot in the `slot_request` queue.
            self._wait_for_slot(lease_secs=lease_secs, timeout=timeout)

            # Record that we (this session id) are working on a key. Expire
            # that note after the lease timeout.
            itemkey = self._itemkey(item)
            logger.info('{} -> {}'.format(self._lease_key_prefix + itemkey,
                                          self._session))
            self._db.setex(self._lease_key_prefix + itemkey,
                           lease_secs,
                           self._session)
        return item
