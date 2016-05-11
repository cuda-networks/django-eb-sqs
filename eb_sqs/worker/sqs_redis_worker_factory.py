from __future__ import absolute_import, unicode_literals

from eb_sqs.aws.sqs_queue_client import SqsQueueClient
from eb_sqs.worker.worker import Worker
from eb_sqs.worker.worker_factory import WorkerFactory


class SqsRedisWorkerFactory(WorkerFactory):
    _WORKER = None # type: Worker

    def __init__(self):
        super(SqsRedisWorkerFactory, self).__init__()

    def create(self):
        if not SqsRedisWorkerFactory._WORKER:
            SqsRedisWorkerFactory._WORKER = Worker(SqsQueueClient(), None)
        return SqsRedisWorkerFactory._WORKER
