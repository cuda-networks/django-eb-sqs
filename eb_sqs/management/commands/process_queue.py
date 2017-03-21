from __future__ import absolute_import, unicode_literals

import boto3
import logging

from django.core.management import BaseCommand, CommandError

from eb_sqs import settings
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

        queue_names = options['queue_names'].split(',')

        logger.debug('Connecting to SQS: {}'.format(', '.join(queue_names)))

        sqs = boto3.resource('sqs', region_name=settings.AWS_REGION)
        queues = [sqs.get_queue_by_name(QueueName=queue_name) for queue_name in queue_names]

        logger.debug('Connected to SQS')

        while True:
            for queue in queues:
                messages = queue.receive_messages(
                    MaxNumberOfMessages=settings.MAX_NUMBER_OF_MESSAGES,
                    WaitTimeSeconds=settings.WAIT_TIME_S,
                )

                for msg in messages:
                    logger.debug('Read message {}'.format(msg.message_id))
                    self._process_message(msg)
                    logger.debug('Processed message {}'.format(msg.message_id))
                    msg.delete()
                    logger.debug('Deleted message {}'.format(msg.message_id))

    def _process_message(self, message):
        # type: (Message) -> None
        worker = WorkerFactory.default().create()
        worker.execute(message.body)
