import logging

from eb_sqs.settings import DEFAULT_DELAY, DEFAULT_QUEUE, EXECUTE_INLINE
from eb_sqs.sqs import SqsClient
from eb_sqs.worker import WorkerTask

logger = logging.getLogger("eb_sqs")
sqs = SqsClient()


def func_delay_decorator(func, queue_name):
    def wrapper(*args, **kwargs):
        queue = queue_name if queue_name else DEFAULT_QUEUE
        execute_inline = kwargs.pop('execute_inline', EXECUTE_INLINE) if kwargs else EXECUTE_INLINE
        delay = kwargs.pop('delay', DEFAULT_DELAY) if kwargs else DEFAULT_DELAY

        worker_task = WorkerTask(func, args, kwargs)

        if execute_inline:
            return worker_task.execute()
        else:
            logger.info('Delaying task %s: %s, %s (%s)', worker_task.abs_func_name, args, kwargs, queue)
            sqs.add_message(queue, worker_task.serialize(), delay)

    return wrapper


class task(object):
    def __init__(self, queue_name=None):
        self.queue_name = queue_name

    def __call__(self, *args, **kwargs):
        func = args[0]
        func.delay = func_delay_decorator(func, self.queue_name)
        return func
