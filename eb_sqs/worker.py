import importlib
import json
import logging

logger = logging.getLogger("eb_sqs")


class WorkerTask:
    def __init__(self, queue, func, args, kwargs, max_retries, retry):
        self.queue = queue
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.max_retries = max_retries
        self.retry = retry

        self.abs_func_name = '{}.{}'.format(self.func.__module__, self.func.func_name)

    def execute(self):
        from eb_sqs.decorators import func_retry_decorator
        self.func.retry = func_retry_decorator(worker_task=self)
        return self.func(*self.args, **self.kwargs)

    def serialize(self):

        task = {
                'queue': self.queue,
                'func': self.abs_func_name,
                'args': self.args,
                'kwargs': self.kwargs,
                'max_retries': self.max_retries,
                'retry': self.retry,
            }

        return json.dumps(task)

    @staticmethod
    def deserialize(msg):
        task = json.loads(msg)

        abs_func_name = task['func']
        func_name = abs_func_name.split(".")[-1]
        func_path = ".".join(abs_func_name.split(".")[:-1])
        func_module = importlib.import_module(func_path)

        func = getattr(func_module, func_name)

        queue = task['queue']
        args = task.get('args', [])
        kwargs = task.get('kwargs', {})
        max_retries = task['max_retries']
        retry = task['retry']

        return WorkerTask(queue, func, args, kwargs, max_retries, retry)


class Worker:
    class InvalidMessageFormat(Exception):
        def __init__(self, msg, caught):
            super(Worker.InvalidMessageFormat, self).__init__()
            self.msg = msg
            self.caught = caught

    class ExecutionFailedException(Exception):
        def __init__(self, task_name, caught):
            super(Worker.ExecutionFailedException, self).__init__()
            self.task_name = task_name
            self.caught = caught

    def __init__(self):
        pass

    def execute(self, msg):
        try:
            worker_task = WorkerTask.deserialize(msg)
        except Exception as ex:
            logger.exception(
                'Message %s is not a valid worker task: %s',
                msg,
                ex
            )

            raise Worker.InvalidMessageFormat(msg, ex)

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

            raise Worker.ExecutionFailedException(worker_task.abs_func_name, ex)
