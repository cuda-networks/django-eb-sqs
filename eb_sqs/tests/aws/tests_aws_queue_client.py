from __future__ import absolute_import, unicode_literals

import time
from unittest import TestCase
from mock import patch

import boto3
from botocore.exceptions import ClientError
from moto import mock_sqs

from eb_sqs import settings
from eb_sqs.aws.sqs_queue_client import SqsQueueClient
from eb_sqs.worker.queue_client import QueueDoesNotExistException


class AwsQueueClientTest(TestCase):
    @mock_sqs()
    def test_add_message(self):
        sqs = boto3.resource('sqs',
                             region_name=settings.AWS_REGION)
        queue = sqs.create_queue(QueueName='eb-sqs-default')

        queue_client = SqsQueueClient()

        queue_client.add_message('default', 'msg', 0)

        queue.reload()
        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '1')

    @mock_sqs()
    def test_add_message_delayed(self):
        delay = 1
        sqs = boto3.resource('sqs',
                             region_name=settings.AWS_REGION)
        queue = sqs.create_queue(QueueName='eb-sqs-default')
        queue_client = SqsQueueClient()

        queue_client.add_message('default', 'msg', delay)

        queue.reload()
        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '0')

        time.sleep(delay + 0.1)

        queue.reload()
        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '1')

    @mock_sqs()
    def test_add_message_wrong_queue(self):
        sqs = boto3.resource('sqs',
                             region_name=settings.AWS_REGION)
        sqs.create_queue(QueueName='default')
        queue_client = SqsQueueClient()

        with self.assertRaises(QueueDoesNotExistException):
            queue_client.add_message('invalid', 'msg', 0)

    @mock_sqs()
    def test_auto_add_queue(self):
        settings.AUTO_ADD_QUEUE = True

        queue_name = 'test-queue'

        sqs = boto3.resource('sqs',
                             region_name=settings.AWS_REGION)

        queue_client = SqsQueueClient()

        queue_client.add_message(queue_name, 'msg', 0)

        full_queue_name = settings.QUEUE_PREFIX + queue_name

        queue = sqs.get_queue_by_name(QueueName=full_queue_name)

        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '1')

        queue.delete()

        # moto throws exception inconsistent with boto, thus the patching
        with patch.object(queue_client.queue_cache[full_queue_name], 'send_message') as send_message_fn:
            send_message_fn.side_effect = ClientError({'Error': {'Code': 'AWS.SimpleQueueService.NonExistentQueue'}}, None)

            queue_client.add_message(queue_name, 'msg', 0)

        queue = sqs.get_queue_by_name(QueueName=full_queue_name)

        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '1')

        settings.AUTO_ADD_QUEUE = False
