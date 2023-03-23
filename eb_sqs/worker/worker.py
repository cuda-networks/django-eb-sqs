from __future__ import absolute_import, unicode_literals

import logging
import uuid
from typing import Any

from eb_sqs import settings
from eb_sqs.worker.queue_client import QueueDoesNotExistException, QueueClient, QueueClientException
from eb_sqs.worker.worker_exceptions import InvalidMessageFormatException, ExecutionFailedException, \
    MaxRetriesReachedException, InvalidQueueException, QueueException
from eb_sqs.worker.worker_task import WorkerTask

logger = logging.getLogger("eb_sqs")


class Worker(object):
    def __init__(self, queue_client: QueueClient):
        super(Worker, self).__init__()
        self.queue_client = queue_client

    def execute(self, msg: str) -> Any:
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
            if settings.DEAD_LETTER_MODE:
                # If in dead letter mode only try to run callback. Do not execute task.
                logger.debug(
                    'Task %s (%s, retry-id: %s) not executed (dead letter queue)',
                    worker_task.abs_func_name,
                    worker_task.id,
                    worker_task.retry_id,
                )

            else:
                logger.debug(
                    'Execute task %s (%s, retry-id: %s) with args: %s and kwargs: %s',
                    worker_task.abs_func_name,
                    worker_task.id,
                    worker_task.retry_id,
                    worker_task.args,
                    worker_task.kwargs
                )

                return self._execute_task(worker_task)
        except QueueException:
            raise
        except MaxRetriesReachedException:
            raise
        except Exception as ex:
            logger.exception(
                'Task %s (%s, retry-id: %s) failed to execute with args: %s and kwargs: %s: %s',
                worker_task.abs_func_name,
                worker_task.id,
                worker_task.retry_id,
                worker_task.args,
                worker_task.kwargs,
                ex
            )

            raise ExecutionFailedException(worker_task.abs_func_name, ex)

    def delay(self, group_id: str, queue_name: str, func: Any, args: tuple, kwargs: dict, max_retries: int, use_pickle: bool,
              delay: int, execute_inline: bool) -> Any:
        worker_task = WorkerTask(str(uuid.uuid4()), group_id, queue_name, func, args, kwargs, max_retries, 0, None,
                                 use_pickle)
        return self._enqueue_task(worker_task, delay, execute_inline, False, True)

    def retry(self, worker_task: WorkerTask, delay: int, execute_inline: bool, count_retries: bool) -> Any:
        worker_task = worker_task.copy(settings.FORCE_SERIALIZATION)
        worker_task.retry_id = str(uuid.uuid4())
        return self._enqueue_task(worker_task, delay, execute_inline, True, count_retries)

    def _enqueue_task(self, worker_task: WorkerTask, delay: int, execute_inline: bool, is_retry: bool,
                      count_retries: bool) -> Any:
        try:
            if is_retry and count_retries:
                worker_task.retry += 1
                if worker_task.retry >= worker_task.max_retries:
                    raise MaxRetriesReachedException(worker_task.retry)

            logger.debug('%s task %s (%s, retry-id: %s): %s, %s (%s%s)',
                         'Retrying' if is_retry else 'Delaying',
                         worker_task.abs_func_name,
                         worker_task.id,
                         worker_task.retry_id,
                         worker_task.args,
                         worker_task.kwargs,
                         worker_task.queue,
                         ', inline' if execute_inline else '')

            if execute_inline:
                return self._execute_task(worker_task)
            else:
                self.queue_client.add_message(worker_task.queue, worker_task.serialize(), delay)
                return None
        except QueueDoesNotExistException as ex:
            raise InvalidQueueException(ex.queue_name)
        except QueueClientException as ex:
            logger.warning('Task %s (%s, retry-id: %s) failed to enqueue to %s: %s',
                           worker_task.abs_func_name,
                           worker_task.id,
                           worker_task.retry_id,
                           worker_task.queue,
                           ex)

            raise QueueException()

    @classmethod
    def _execute_task(cls, worker_task: WorkerTask) -> Any:
        result = worker_task.execute()
        return result
