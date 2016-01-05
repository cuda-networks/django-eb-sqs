import importlib
import json
import logging

logger = logging.getLogger("eb_sqs")


class WorkerTask:
    def __init__(self, func, args, kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

        self.abs_func_name = '{}.{}'.format(self.func.__module__, self.func.func_name)

    def execute(self):
        return self.func(*self.args, **self.kwargs)

    def serialize(self):

        task = {
                'func': self.abs_func_name,
                'args': self.args,
                'kwargs': self.kwargs,
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

        args = task.get('args', [])
        kwargs = task.get('kwargs', {})

        return WorkerTask(func, args, kwargs)


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
