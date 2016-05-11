from __future__ import absolute_import, unicode_literals

from eb_sqs.settings import DEFAULT_DELAY, DEFAULT_QUEUE, EXECUTE_INLINE, DEFAULT_MAX_RETRIES, USE_PICKLE
from eb_sqs.worker.worker_factory import WorkerFactory


def func_delay_decorator(func, queue_name, max_retries_count, use_pickle):
    # type: (Any, unicode, int, bool) -> (tuple, dict)
    def wrapper(*args, **kwargs):
        # type: (tuple, dict) -> Any
        queue = queue_name if queue_name else DEFAULT_QUEUE
        max_retries = max_retries_count if max_retries_count else DEFAULT_MAX_RETRIES
        pickle = use_pickle if use_pickle else USE_PICKLE

        execute_inline = kwargs.get('execute_inline', EXECUTE_INLINE) if kwargs else EXECUTE_INLINE
        delay = kwargs.get('delay', DEFAULT_DELAY) if kwargs else DEFAULT_DELAY
        group_id = kwargs.get('group_id')

        worker = WorkerFactory.default().create()
        return worker.delay(group_id, queue, func, args, kwargs, max_retries, pickle, delay, execute_inline)

    return wrapper


def func_retry_decorator(worker_task):
    # type: (WorkerTask) -> (tuple, dict)
    def wrapper(*args, **kwargs):
        # type: (tuple, dict) -> Any
        execute_inline = kwargs.get('execute_inline', EXECUTE_INLINE) if kwargs else EXECUTE_INLINE
        delay = kwargs.get('delay', DEFAULT_DELAY) if kwargs else DEFAULT_DELAY

        worker = WorkerFactory.default().create()
        return worker.retry(worker_task, delay, execute_inline)
    return wrapper


class task(object):
    def __init__(self, queue_name=None, max_retries=None, use_pickle=None):
        # type: (unicode, int, bool) -> None
        self.queue_name = queue_name
        self.max_retries = max_retries
        self.use_pickle = use_pickle

    def __call__(self, *args, **kwargs):
        # type: (tuple, dict) -> Any
        func = args[0]
        func.retry_num = 0
        func.delay = func_delay_decorator(func, self.queue_name, self.max_retries, self.use_pickle)
        return func
