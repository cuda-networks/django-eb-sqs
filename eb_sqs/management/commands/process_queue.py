from __future__ import absolute_import, unicode_literals

from datetime import timedelta, datetime

import boto3
import logging

from botocore.config import Config
from django.core.management import BaseCommand, CommandError
from django.utils import timezone

from eb_sqs import settings
from eb_sqs.worker.worker import Worker
from eb_sqs.worker.worker_factory import WorkerFactory

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Command to process tasks from one or more SQS queues'

    PREFIX_STR = 'prefix:'

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

        prefixes = list(filter(lambda qn: qn.startsWith(self.PREFIX_STR), queue_names))
        queues = self._get_queues_by_names(sqs, list(set(queue_names) - set(prefixes)))

        queue_prefixes = [prefix.split(self.PREFIX_STR)[1] for prefix in prefixes]
        static_queues = queues
        last_update_time = timezone.make_aware(datetime.min)

        logger.debug('[django-eb-sqs] Connected to SQS: {}'.format(', '.join(queue_names)))

        worker = WorkerFactory.default().create()

        while True:
            if len(queue_prefixes) > 0 and \
                    timezone.now() - timedelta(seconds=settings.REFRESH_PREFIX_QUEUES_S) > last_update_time:
                queues = static_queues + self._get_queues_by_prefixes(sqs, queue_prefixes)
                last_update_time = timezone.now()
                logger.debug('[django-eb-sqs] Updated SQS queues: {}'.format(
                    ', '.join([queue.url for queue in queues])
                ))

            for queue in queues:
                try:
                    messages = queue.receive_messages(
                        MaxNumberOfMessages=settings.MAX_NUMBER_OF_MESSAGES,
                        WaitTimeSeconds=settings.WAIT_TIME_S,
                    )

                    for msg in messages:
                        self._process_message(msg, worker)
                except Exception as exc:
                    logger.warning('[django-eb-sqs] Error polling queue {}: {}'.format(queue.url, exc), exc_info=1)

    @staticmethod
    def _process_message(msg, worker):
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

    @staticmethod
    def _get_queues_by_names(sqs, queue_names):
        return [sqs.get_queue_by_name(QueueName=queue_name) for queue_name in queue_names]

    @staticmethod
    def _get_queues_by_prefixes(sqs, prefixes):
        queues = []

        for prefix in prefixes:
            queues += sqs.queues.filter(QueueNamePrefix=prefix)

        return queues
