from __future__ import absolute_import, unicode_literals

import logging

from eb_sqs.settings import FORCE_SERIALIZATION
from eb_sqs.worker.queue_client import QueueDoesNotExistException, QueueClient
from eb_sqs.worker.worker_exceptions import InvalidMessageFormatException, ExecutionFailedException, \
    MaxRetriesReachedException, InvalidQueueException
from eb_sqs.worker.worker_task import WorkerTask

logger = logging.getLogger("eb_sqs")


class Worker(object):
    def __init__(self, queue_client):
        # type: (QueueClient) -> None
        super(Worker, self).__init__()
        self.queue_client = queue_client

    def execute(self, msg):
        # type: (unicode) -> Any
        try:
            worker_task = WorkerTask.deserialize(msg)
        except Exception as ex:
            logger.exception(
                'Message %s is not a valid worker task: %s',
                msg,
                ex
            )

            raise InvalidMessageFormatException(msg, ex)

        try:
            logger.info(
                'Execute task %s with args: %s and kwargs: %s',
                worker_task.abs_func_name,
                worker_task.args,
                worker_task.kwargs
            )

            return worker_task.execute()
        except Exception as ex:
            logger.exception(
                'Task %s failed to execute with args: %s and kwargs: %s: %s',
                worker_task.abs_func_name,
                worker_task.args,
                worker_task.kwargs,
                ex
            )

            raise ExecutionFailedException(worker_task.abs_func_name, ex)

    def enqueue(self, worker_task, delay, execute_inline, is_retry=False):
        # type: (WorkerTask, unicode, int, bool) -> Any
        try:
            if is_retry:
                worker_task.retry += 1
                if worker_task.retry > worker_task.max_retries:
                    raise MaxRetriesReachedException(worker_task.retry)

            if execute_inline:
                if FORCE_SERIALIZATION:
                    return WorkerTask.deserialize(worker_task.serialize()).execute()
                else:
                    return worker_task.execute()
            else:
                logger.info('%s task %s: %s, %s (%s)',
                            'Retrying' if is_retry else 'Delaying',
                            worker_task.abs_func_name,
                            worker_task.args,
                            worker_task.kwargs,
                            worker_task.queue)

                self.queue_client.add_message(worker_task.queue, worker_task.serialize(), delay)
                return None
        except QueueDoesNotExistException as ex:
            raise InvalidQueueException(ex.queue_name)
