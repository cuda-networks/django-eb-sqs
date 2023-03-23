from abc import ABCMeta, abstractmethod

from eb_sqs import settings
from eb_sqs.worker.worker import Worker


class WorkerFactory(metaclass=ABCMeta):
    @abstractmethod
    def create(self) -> Worker:
        pass

    @staticmethod
    def default():
        if not settings.WORKER_FACTORY:
            from eb_sqs.worker.sqs_worker_factory import SqsWorkerFactory
            return SqsWorkerFactory()
        else:
            return settings.WORKER_FACTORY
