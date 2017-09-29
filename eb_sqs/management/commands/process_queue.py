from __future__ import absolute_import, unicode_literals

import boto3
import logging

from botocore.config import Config
from django.core.management import BaseCommand, CommandError

from eb_sqs import settings
from eb_sqs.worker.worker import Worker
from eb_sqs.worker.worker_factory import WorkerFactory

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Command to process tasks from one or more SQS queues'

    def add_arguments(self, parser):
        parser.add_argument('--queues', '-q',
                            dest='queue_names',
                            help='Name of queues to process, separated by commas')

    def handle(self, *args, **options):
        if not options['queue_names']:
            raise CommandError('Queue names (--queues) not specified')

        queue_names = [queue_name.rstrip() for queue_name in options['queue_names'].split(',')]

        logger.debug('[django-eb-sqs] Connecting to SQS: {}'.format(', '.join(queue_names)))

        sqs = boto3.resource(
            'sqs',
            region_name=settings.AWS_REGION,
            config=Config(retries={'max_attempts': settings.AWS_MAX_RETRIES})
        )
        queues = [sqs.get_queue_by_name(QueueName=queue_name) for queue_name in queue_names]

        logger.debug('[django-eb-sqs] Connected to SQS: {}'.format(', '.join(queue_names)))

        worker = WorkerFactory.default().create()

        while True:
            for queue in queues:
                messages = queue.receive_messages(
                    MaxNumberOfMessages=settings.MAX_NUMBER_OF_MESSAGES,
                    WaitTimeSeconds=settings.WAIT_TIME_S,
                )

                for msg in messages:
                    self._process_message(msg, worker)

    def _process_message(self, msg, worker):
        # type: (Any, Worker) -> None
        logger.debug('[django-eb-sqs] Read message {}'.format(msg.message_id))
        try:
            worker.execute(msg.body)
            logger.debug('[django-eb-sqs] Processed message {}'.format(msg.message_id))
        except Exception as exc:
            logger.error('[django-eb-sqs] Unhandled error: {}'.format(exc), exc_info=1)
        finally:
            msg.delete()
            logger.debug('[django-eb-sqs] Deleted message {}'.format(msg.message_id))
