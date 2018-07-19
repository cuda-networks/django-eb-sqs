from __future__ import absolute_import, unicode_literals

import base64
import importlib
import json
import uuid

from eb_sqs import settings

try:
    import cPickle as pickle
except Exception:
    import pickle


class WorkerTask(object):
    def __init__(self, id, group_id, queue, func, args, kwargs, max_retries, retry, retry_id, use_pickle):
        # type: (str, unicode, unicode, Any, tuple, dict, int, int, unicode, bool) -> None
        super(WorkerTask, self).__init__()
        self.id = id
        self.group_id = group_id
        self.queue = queue
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.max_retries = max_retries
        self.retry = retry
        self.retry_id = retry_id
        self.use_pickle = use_pickle

        self.abs_func_name = '{}.{}'.format(self.func.__module__, self.func.__name__)

    def execute(self):
        # type: () -> Any
        from eb_sqs.decorators import func_retry_decorator
        self.func.retry_num = self.retry
        self.func.retry = func_retry_decorator(worker_task=self)
        return self.func(*self.args, **self.kwargs)

    def serialize(self):
        # type: () -> unicode
        args = WorkerTask._pickle_args(self.args) if self.use_pickle else self.args
        kwargs = WorkerTask._pickle_args(self.kwargs) if self.use_pickle else self.kwargs

        task = {
                'id': self.id,
                'groupId': self.group_id,
                'queue': self.queue,
                'func': self.abs_func_name,
                'args': args,
                'kwargs': kwargs,
                'maxRetries': self.max_retries,
                'retry': self.retry,
                'retryId': self.retry_id,
                'pickle': self.use_pickle,
            }

        return json.dumps(task)

    def copy(self, use_serialization):
        # type: (bool) -> WorkerTask
        if use_serialization:
            return WorkerTask.deserialize(self.serialize())
        else:
            return WorkerTask(
                self.id,
                self.group_id,
                self.queue,
                self.func,
                self.args,
                self.kwargs,
                self.max_retries,
                self.retry,
                self.retry_id,
                self.use_pickle,
            )

    @staticmethod
    def _pickle_args(args):
        # type: (Any) -> unicode
        return base64.b64encode(pickle.dumps(args, pickle.HIGHEST_PROTOCOL)).decode('utf-8')

    @staticmethod
    def deserialize(msg):
        # type: (unicode) -> WorkerTask
        task = json.loads(msg)

        id = task.get('id', str(uuid.uuid4()))
        group_id = task.get('groupId')

        abs_func_name = task['func']
        func_name = abs_func_name.split(".")[-1]
        func_path = ".".join(abs_func_name.split(".")[:-1])
        func_module = importlib.import_module(func_path)

        func = getattr(func_module, func_name)

        use_pickle = task.get('pickle', False)
        queue = task.get('queue', settings.DEFAULT_QUEUE)

        task_args = task.get('args', [])
        args = WorkerTask._unpickle_args(task_args) if use_pickle else task_args

        kwargs = WorkerTask._unpickle_args(task['kwargs']) if use_pickle else task['kwargs']

        max_retries = task.get('maxRetries', settings.DEFAULT_MAX_RETRIES)
        retry = task.get('retry', 0)
        retry_id = task.get('retryId')

        return WorkerTask(id, group_id, queue, func, args, kwargs, max_retries, retry, retry_id, use_pickle)

    @staticmethod
    def _unpickle_args(args):
        # type: (unicode) -> dict
        return pickle.loads(base64.b64decode(args.encode('utf-8')))
