from __future__ import absolute_import, unicode_literals

from eb_sqs.aws.sqs_queue_client import SqsQueueClient
from eb_sqs.worker.worker import Worker
from eb_sqs.worker.worker_factory import WorkerFactory


class SqsWorkerFactory(WorkerFactory):
    _WORKER = None # type: Worker

    def __init__(self):
        super(SqsWorkerFactory, self).__init__()

    def create(self):
        if not SqsWorkerFactory._WORKER:
            SqsWorkerFactory._WORKER = Worker(SqsQueueClient())
        return SqsWorkerFactory._WORKER
