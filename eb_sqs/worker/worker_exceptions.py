from __future__ import absolute_import, unicode_literals


class WorkerException(Exception):
    pass


class InvalidMessageFormatException(WorkerException):
    def __init__(self, msg: str, caught: Exception):
        super(InvalidMessageFormatException, self).__init__()
        self.msg = msg
        self.caught = caught


class ExecutionFailedException(WorkerException):
    def __init__(self, task_name: str, caught: Exception):
        super(ExecutionFailedException, self).__init__()
        self.task_name = task_name
        self.caught = caught


class MaxRetriesReachedException(WorkerException):
    def __init__(self, retries: int):
        super(MaxRetriesReachedException, self).__init__()
        self.retries = retries


class QueueException(WorkerException):
        pass


class InvalidQueueException(QueueException):
    def __init__(self, queue_name: str):
        super(InvalidQueueException, self).__init__()
        self.queue_name = queue_name
