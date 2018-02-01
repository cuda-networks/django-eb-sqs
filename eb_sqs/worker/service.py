from __future__ import absolute_import, unicode_literals

from datetime import timedelta, datetime

import boto3
import logging

from botocore.config import Config
from django.utils import timezone

from eb_sqs import settings
from eb_sqs.worker.worker import Worker
from eb_sqs.worker.worker_factory import WorkerFactory

logger = logging.getLogger(__name__)


class WorkerService(object):
    PREFIX_STR = 'prefix:'

    def process_queues(self, queue_names):
        logger.debug('[django-eb-sqs] Connecting to SQS: {}'.format(', '.join(queue_names)))

        sqs = boto3.resource(
            'sqs',
            region_name=settings.AWS_REGION,
            config=Config(retries={'max_attempts': settings.AWS_MAX_RETRIES})
        )

        prefixes = list(filter(lambda qn: qn.startswith(self.PREFIX_STR), queue_names))
        queues = self.get_queues_by_names(sqs, list(set(queue_names) - set(prefixes)))

        queue_prefixes = [prefix.split(self.PREFIX_STR)[1] for prefix in prefixes]
        static_queues = queues
        last_update_time = timezone.make_aware(datetime.min)

        logger.debug('[django-eb-sqs] Connected to SQS: {}'.format(', '.join(queue_names)))

        worker = WorkerFactory.default().create()

        logger.info('[django-eb-sqs] WAIT_TIME_S = {}'.format(settings.WAIT_TIME_S))
        logger.info('[django-eb-sqs] MAX_NUMBER_OF_MESSAGES = {}'.format(settings.MAX_NUMBER_OF_MESSAGES))
        logger.info('[django-eb-sqs] AUTO_ADD_QUEUE = {}'.format(settings.AUTO_ADD_QUEUE))
        logger.info('[django-eb-sqs] QUEUE_PREFIX = {}'.format(settings.QUEUE_PREFIX))
        logger.info('[django-eb-sqs] DEFAULT_QUEUE = {}'.format(settings.DEFAULT_QUEUE))
        logger.info('[django-eb-sqs] DEFAULT_MAX_RETRIES = {}'.format(settings.DEFAULT_MAX_RETRIES))
        logger.info('[django-eb-sqs] REFRESH_PREFIX_QUEUES_S = {}'.format(settings.REFRESH_PREFIX_QUEUES_S))

        while True:
            if len(queue_prefixes) > 0 and \
                    timezone.now() - timedelta(seconds=settings.REFRESH_PREFIX_QUEUES_S) > last_update_time:
                queues = static_queues + self.get_queues_by_prefixes(sqs, queue_prefixes)
                last_update_time = timezone.now()
                logger.info('[django-eb-sqs] Updated SQS queues: {}'.format(
                    ', '.join([queue.url for queue in queues])
                ))

            self.process_messages(queues, worker)

    def process_messages(self, queues, worker):
        for queue in queues:
            try:
                messages = self.poll_messages(queue)

                for msg in messages:
                    self.process_message(msg, worker)
            except Exception as exc:
                logger.warning('[django-eb-sqs] Error polling queue {}: {}'.format(queue.url, exc), exc_info=1)

    def poll_messages(self, queue):
        return queue.receive_messages(
            MaxNumberOfMessages=settings.MAX_NUMBER_OF_MESSAGES,
            WaitTimeSeconds=settings.WAIT_TIME_S,
        )

    def process_message(self, msg, worker):
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

    def get_queues_by_names(self, sqs, queue_names):
        return [sqs.get_queue_by_name(QueueName=queue_name) for queue_name in queue_names]

    def get_queues_by_prefixes(self, sqs, prefixes):
        queues = []

        for prefix in prefixes:
            queues += sqs.queues.filter(QueueNamePrefix=prefix)

        return queues