from __future__ import absolute_import, unicode_literals

import base64
import importlib
import json

try:
   import cPickle as pickle
except:
   import pickle


class WorkerTask(object):
    def __init__(self, queue, func, args, kwargs, max_retries, retry, use_pickle):
        # type: (unicode, Any, tuple, dict, int, int, bool) -> None
        super(WorkerTask, self).__init__()
        self.queue = queue
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.max_retries = max_retries
        self.retry = retry
        self.use_pickle = use_pickle

        self.abs_func_name = '{}.{}'.format(self.func.__module__, self.func.func_name)

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
                'queue': self.queue,
                'func': self.abs_func_name,
                'args': args,
                'kwargs': kwargs,
                'max_retries': self.max_retries,
                'retry': self.retry,
                'pickle': self.use_pickle,
            }

        return json.dumps(task)

    @staticmethod
    def _pickle_args(args):
        # type: (dict) -> unicode
        return base64.b64encode(pickle.dumps(args, pickle.HIGHEST_PROTOCOL))

    @staticmethod
    def deserialize(msg):
        # type: (unicode) -> WorkerTask
        task = json.loads(msg)

        abs_func_name = task['func']
        func_name = abs_func_name.split(".")[-1]
        func_path = ".".join(abs_func_name.split(".")[:-1])
        func_module = importlib.import_module(func_path)

        func = getattr(func_module, func_name)

        use_pickle = task.get('pickle', False)
        queue = task['queue']
        args = WorkerTask._unpickle_args(task['args']) if use_pickle else task['args']
        kwargs = WorkerTask._unpickle_args(task['kwargs']) if use_pickle else task['kwargs']
        max_retries = task['max_retries']
        retry = task['retry']

        return WorkerTask(queue, func, args, kwargs, max_retries, retry, use_pickle)

    @staticmethod
    def _unpickle_args(args):
        # type: (unicode) -> dict
        return pickle.loads(base64.b64decode(args))
