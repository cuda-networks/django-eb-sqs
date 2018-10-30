from __future__ import absolute_import, unicode_literals

from eb_sqs import settings
from eb_sqs.worker.worker_factory import WorkerFactory
from eb_sqs.worker.worker_task import WorkerTask


def _get_kwarg_val(kwargs, key, default):
    # type: (dict, str, Any) -> Any
    return kwargs.pop(key, default) if kwargs else default


def func_delay_decorator(func, queue_name, max_retries_count, use_pickle):
    # type: (Any, str, int, bool) -> (tuple, dict)
    def wrapper(*args, **kwargs):
        # type: (tuple, dict) -> Any
        queue = _get_kwarg_val(kwargs, 'queue_name', queue_name if queue_name else settings.DEFAULT_QUEUE)
        max_retries = _get_kwarg_val(kwargs, 'max_retries', max_retries_count if max_retries_count else settings.DEFAULT_MAX_RETRIES)
        pickle = _get_kwarg_val(kwargs, 'use_pickle', use_pickle if use_pickle else settings.USE_PICKLE)

        execute_inline = _get_kwarg_val(kwargs, 'execute_inline', False) or settings.EXECUTE_INLINE
        delay = _get_kwarg_val(kwargs, 'delay',  settings.DEFAULT_DELAY)
        group_id = _get_kwarg_val(kwargs, 'group_id', None)

        worker = WorkerFactory.default().create()
        return worker.delay(group_id, queue, func, args, kwargs, max_retries, pickle, delay, execute_inline)

    return wrapper


def func_retry_decorator(worker_task):
    # type: (WorkerTask) -> (tuple, dict)
    def wrapper(*args, **kwargs):
        # type: (tuple, dict) -> Any
        execute_inline = _get_kwarg_val(kwargs, 'execute_inline', False) or settings.EXECUTE_INLINE
        delay = _get_kwarg_val(kwargs, 'delay', settings.DEFAULT_DELAY)
        count_retries = _get_kwarg_val(kwargs, 'count_retries', settings.DEFAULT_COUNT_RETRIES)

        worker = WorkerFactory.default().create()
        return worker.retry(worker_task, delay, execute_inline, count_retries)
    return wrapper


class task(object):
    def __init__(self, queue_name=None, max_retries=None, use_pickle=None):
        # type: (str, int, bool) -> None
        self.queue_name = queue_name
        self.max_retries = max_retries
        self.use_pickle = use_pickle

    def __call__(self, *args, **kwargs):
        # type: (tuple, dict) -> Any
        func = args[0]
        func.retry_num = 0
        func.delay = func_delay_decorator(func, self.queue_name, self.max_retries, self.use_pickle)
        return func
