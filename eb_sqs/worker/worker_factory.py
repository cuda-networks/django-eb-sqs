from __future__ import absolute_import, unicode_literals

from abc import ABCMeta, abstractmethod

from eb_sqs import settings
from eb_sqs.worker.worker import Worker


class WorkerFactory(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        # type: () -> None
        super(WorkerFactory, self).__init__()

    @abstractmethod
    def create(self):
        # type: () -> Worker
        pass

    @staticmethod
    def default():
        # type: () -> WorkerFactory
        if not settings.WORKER_FACTORY:
            from eb_sqs.worker.sqs_worker_factory import SqsWorkerFactory
            return SqsWorkerFactory()
        else:
            return settings.WORKER_FACTORY
