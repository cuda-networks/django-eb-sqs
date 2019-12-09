from __future__ import absolute_import, unicode_literals

import boto3
from botocore.config import Config
from django.conf import settings
from django.core.management import BaseCommand, CommandError
import requests
from requests.exceptions import ConnectionError


class Command(BaseCommand):
    help = "Command to run the EB SQS worker"

    def add_arguments(self, parser):
        parser.add_argument('--url', '-u',
                            dest='url',
                            help='Url of the worker endpoint API')

        parser.add_argument('--queue', '-q',
                            dest='queue_name',
                            help='Name of the queue to process')

        parser.add_argument('--retries', '-r',
                            dest='retry_limit',
                            default=10,
                            help='Retry limit until the message is discarded (default 10)')

    def handle(self, *args, **options):
        if not options['url']:
            raise CommandError('Worker endpoint url parameter (--url) not found')

        if not options['queue_name']:
            raise CommandError('Queue name (--queue) not specified')

        url = options['url']
        queue_name = options['queue_name']
        retry_limit = max(int(options['retry_limit']), 1)

        try:
            self.stdout.write('Connect to SQS')
            sqs = boto3.resource(
                'sqs',
                region_name=settings.AWS_REGION,
                endpoint_url=settings.SQS_ENDPOINT_URL,
                config=Config(retries={'max_attempts': settings.AWS_MAX_RETRIES})
            )
            queue = sqs.get_queue_by_name(QueueName=queue_name)
            self.stdout.write('> Connected')

            while True:
                messages = queue.receive_messages(
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20
                )

                if len(messages) == 0:
                    break

                for msg in messages:
                    self.stdout.write('Deliver message {}'.format(msg.message_id))
                    if self._process_message_with_retry(url, retry_limit, msg):
                        self.stdout.write('> Delivered')
                    else:
                        self.stdout.write('> Delivery failed (retry-limit reached)')
                    msg.delete()

            self.stdout.write('Message processing finished')
        except ConnectionError:
            self.stdout.write('Connection to {} failed. Message processing failed'.format(url))

    def _process_message_with_retry(self, url, retry_limit, message):
        for retry in range(retry_limit):
            if self._process_message(url, message):
                return True

        return False

    def _process_message(self, url, message):
        response = requests.post(url, data=message.body)
        return response.status_code == 200
