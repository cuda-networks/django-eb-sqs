from __future__ import absolute_import, unicode_literals


class WorkerException(Exception):
    pass


class InvalidMessageFormatException(WorkerException):
    def __init__(self, msg, caught):
        # type: (unicode, Exception) -> None
        super(InvalidMessageFormatException, self).__init__()
        self.msg = msg
        self.caught = caught


class ExecutionFailedException(WorkerException):
    def __init__(self, task_name, caught):
        # type: (unicode, Exception) -> None
        super(ExecutionFailedException, self).__init__()
        self.task_name = task_name
        self.caught = caught


class MaxRetriesReachedException(WorkerException):
    def __init__(self, retries):
        # type: (int) -> None
        super(MaxRetriesReachedException, self).__init__()
        self.retries = retries


class QueueException(WorkerException):
        pass


class InvalidQueueException(QueueException):
    def __init__(self, queue_name):
        # type: (unicode) -> None
        super(InvalidQueueException, self).__init__()
        self.queue_name = queue_name
