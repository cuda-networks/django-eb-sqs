import logging

from eb_sqs.settings import DEFAULT_DELAY, DEFAULT_QUEUE, EXECUTE_INLINE, DEFAULT_MAX_RETRIES
from eb_sqs.sqs import SqsClient
from eb_sqs.worker import WorkerTask

logger = logging.getLogger("eb_sqs")
sqs = SqsClient()


def func_delay_decorator(func, queue_name, max_retries_count):
    def wrapper(*args, **kwargs):
        queue = queue_name if queue_name else DEFAULT_QUEUE
        execute_inline = kwargs.pop('execute_inline', EXECUTE_INLINE) if kwargs else EXECUTE_INLINE
        delay = kwargs.pop('delay', DEFAULT_DELAY) if kwargs else DEFAULT_DELAY
        max_retries = max_retries_count if max_retries_count else DEFAULT_MAX_RETRIES

        worker_task = WorkerTask(queue, func, args, kwargs, max_retries, 0)

        if execute_inline:
            return worker_task.execute()
        else:
            logger.info('Delaying task %s: %s, %s (%s)', worker_task.abs_func_name, args, kwargs, queue)
            sqs.add_message(queue, worker_task.serialize(), delay)

    return wrapper


class MaxRetriesReachedException(Exception):
        def __init__(self, retries):
            super(MaxRetriesReachedException, self).__init__()
            self.retries = retries


def func_retry_decorator(worker_task):
    def wrapper(*args, **kwargs):
        worker_task.retry += 1
        if worker_task.retry > worker_task.max_retries:
            raise MaxRetriesReachedException(worker_task.retry)

        execute_inline = kwargs.pop('execute_inline', EXECUTE_INLINE) if kwargs else EXECUTE_INLINE
        delay = kwargs.pop('delay', DEFAULT_DELAY) if kwargs else DEFAULT_DELAY

        if execute_inline:
            return worker_task.execute()
        else:
            logger.info('Retrying task %s: %s, %s (%s)', worker_task.abs_func_name, worker_task.args, worker_task.kwargs, worker_task.queue)
            sqs.add_message(worker_task.queue, worker_task.serialize(), delay)

    return wrapper


class task(object):
    def __init__(self, queue_name=None, max_retries=None):
        self.queue_name = queue_name
        self.max_retries = max_retries

    def __call__(self, *args, **kwargs):
        func = args[0]
        func.delay = func_delay_decorator(func, self.queue_name, self.max_retries)
        return func
