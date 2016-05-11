from __future__ import absolute_import, unicode_literals

import logging

from eb_sqs.settings import FORCE_SERIALIZATION, GROUP_CALLBACK_TASK
from eb_sqs.worker.group_client import GroupClient
from eb_sqs.worker.queue_client import QueueDoesNotExistException, QueueClient, QueueClientException
from eb_sqs.worker.worker_exceptions import InvalidMessageFormatException, ExecutionFailedException, \
    MaxRetriesReachedException, InvalidQueueException, QueueException
from eb_sqs.worker.worker_task import WorkerTask

logger = logging.getLogger("eb_sqs")


class Worker(object):
    def __init__(self, queue_client, group_client):
        # type: (QueueClient, GroupClient) -> None
        super(Worker, self).__init__()
        self.queue_client = queue_client
        self.group_client = group_client

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
                'Execute task %s (%s) with args: %s and kwargs: %s',
                worker_task.abs_func_name,
                worker_task.id,
                worker_task.args,
                worker_task.kwargs
            )

            return self._execute_task(worker_task)
        except Exception as ex:
            logger.exception(
                'Task %s (%s) failed to execute with args: %s and kwargs: %s: %s',
                worker_task.abs_func_name,
                worker_task.id,
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
                    self._group_callback(worker_task)
                    raise MaxRetriesReachedException(worker_task.retry)

            self.group_client.add(worker_task)

            if execute_inline:
                if FORCE_SERIALIZATION:
                    return self._execute_task(WorkerTask.deserialize(worker_task.serialize()))
                else:
                    return self._execute_task(worker_task)
            else:
                logger.info('%s task %s (%s): %s, %s (%s)',
                            'Retrying' if is_retry else 'Delaying',
                            worker_task.abs_func_name,
                            worker_task.id,
                            worker_task.args,
                            worker_task.kwargs,
                            worker_task.queue)

                self.queue_client.add_message(worker_task.queue, worker_task.serialize(), delay)
                return None
        except QueueDoesNotExistException as ex:
            self._group_callback(worker_task)
            raise InvalidQueueException(ex.queue_name)
        except QueueClientException as ex:
            self._group_callback(worker_task)

            logger.exception('Task %s (%s) failed to enqueue to %s: %s',
                        worker_task.abs_func_name,
                        worker_task.id,
                        worker_task.queue,
                        ex)

            raise QueueException()

    def _execute_task(self, worker_task):
        # type: (WorkerTask) -> Any
        result = worker_task.execute()
        self._group_callback(worker_task)
        return result

    def _group_callback(self, worker_task):
        # type: (WorkerTask) -> None
        if self.group_client.remove(worker_task) and GROUP_CALLBACK_TASK:
            logger.info('Tasks in group %s executed. Trigger callback.')
            GROUP_CALLBACK_TASK.delay(group_id=worker_task.group_id)
