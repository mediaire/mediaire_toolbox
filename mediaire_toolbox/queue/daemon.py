import os
import signal
import logging
import traceback

from abc import ABC, abstractmethod

from mediaire_toolbox.queue.redis_wq import RedisWQ
from mediaire_toolbox.queue import tasks

logger = logging.getLogger(__name__)


"""
Common interface for creating operative daemons that consume from one of 
our queues.
"""

# All daemons run under this configured shared_data folder in docker-compose
# It would be nicer to depend on md_commons to fetch this path, but then
# we would have a circular dependency (toolbox -> commons -> toolbox).
ASSUMED_SHARED_DATA = '/src/shared_data'


class QueueDaemon(ABC):

    def __init__(self,
                 input_queue: RedisWQ,
                 result_queue: RedisWQ,
                 lease_secs: int,
                 daemon_name: str,
                 config: dict):
        """
        Parameters
        ----------

        input_queue:
            A Redis queue instance from which we will consume Tasks
        result_queue:
            The output queue for the daemon, if applicable
        lease_secs:
            Lease timeout in seconds when consuming from the queue
        daemon_name:
            A unique identifier for this daemon, will be used for logging
        config:
            A configuration dictionary with all the necessary extra parameters
            for this daemon.
        """
        self.input_queue = input_queue
        self.result_queue = result_queue
        self.lease_secs = lease_secs
        self.daemon_name = daemon_name
        self.config = config
        self.stopped = False
        self.processing_t_id = None

        try:
            signal.signal(signal.SIGINT, self.exit_gracefully)
            signal.signal(signal.SIGTERM, self.exit_gracefully)
        except ValueError:
            logger.warn('Catching ValueError from signal, are you using '
                        'daemons outside of the main thread, and if so, '
                        'do you know what you are doing?')
            # (signal only works in main thread)
            pass

    @abstractmethod
    def process_task(self, task):
        """
        Business logic to be implemented by the daemon, receiving an already
        deserialized task here.
        """
        pass

    def set_processing_t_id(self, t_id: int):
        self.processing_t_id = t_id

    def run_once(self):
        logger.info('Waiting for items from queue {}'.format(
            self.input_queue._main_q_key))

        limit = self.config.get('lease_limit', -1)
        limit_timeunit = self.config.get('limit_timeunit', 'hour')
        item = self.input_queue.lease(
            lease_secs=self.lease_secs, block=True,
            limit=limit, timeunit=limit_timeunit)
        try:
            # TODO Make this class a parameter for better generalization
            # how to do reflection in python?
            task = tasks.Task().read_bytes(item)
        except Exception as e:
            logger.exception(
                "Operating error or error deserializing task object")
            tb = traceback.format_exc()
            # default to error queue
            self.input_queue.error(item,
                                   msg="{} --> in '{}': {}"
                                       "".format(e, __file__, tb))
            return

        try:
            if task.t_id:
                self.set_processing_t_id(task.t_id)
            self.process_task(task)
            self.input_queue.complete(item)
        except Exception as e:
            t_id = task.t_id if task.t_id else -1
            logger.exception(
                "transaction={} Error processing task in {}"
                .format(t_id, self.daemon_name))
            tb = traceback.format_exc()
            msg = "{} --> in '{}': {}".format(e, __file__, tb)
            if task.t_id and self.result_queue:
                # send the task back to the task manager with an error
                # so the task manager can decide what to do with it
                # for example executing a subflow or simply marking the
                # transaction as failed in db
                task.error = msg
                self.result_queue.put(task.to_bytes())
            else:
                # if the task doesn't yet have a transactionid, default
                # to error queue
                self.input_queue.error(item, msg=msg)
        finally:
            self.set_processing_t_id(None)

    def run(self):
        while not self.stopped:
            self.run_once()

    def exit_gracefully(self, _, __):
        logger.info("Ok, no rush, people. Terminating gracefully now.")
        if self.processing_t_id:
            logger.warn('Processing t_id {} should be properly cancelled!'.
                        format(self.processing_t_id))
            if os.path.exists(ASSUMED_SHARED_DATA):
                logger.warn('Writing a cancellation file in assumed '
                            'shared data folder {}'.format(
                                ASSUMED_SHARED_DATA))
                c_file = os.path.join(ASSUMED_SHARED_DATA,
                                      'cancel-{}'.format(self.processing_t_id))
                if not os.path.exists(c_file):
                    open(c_file, 'a').close()
            else:
                raise Exception('Transaction cancelled due to shutdown')

    def stop(self):
        self.stopped = True
