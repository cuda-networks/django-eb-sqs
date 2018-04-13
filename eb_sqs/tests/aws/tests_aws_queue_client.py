from __future__ import absolute_import, unicode_literals

import time
from unittest import TestCase, skip

import boto3
from moto import mock_sqs

from eb_sqs import settings
from eb_sqs.aws.sqs_queue_client import SqsQueueClient
from eb_sqs.worker.queue_client import QueueDoesNotExistException


class AwsQueueClientTest(TestCase):
    @mock_sqs()
    def test_add_message(self):
        sqs = boto3.resource('sqs')
        queue = sqs.create_queue(QueueName='eb-sqs-default')

        queue_client = SqsQueueClient()

        queue_client.add_message('default', 'msg', 0)

        queue.reload()
        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '1')

    @mock_sqs()
    def test_add_message_delayed(self):
        delay = 1
        sqs = boto3.resource('sqs')
        queue = sqs.create_queue(QueueName='eb-sqs-default')
        queue_client = SqsQueueClient()

        queue_client.add_message('default', 'msg', delay)

        queue.reload()
        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '0')

        time.sleep(delay + 0.1)

        queue.reload()
        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '1')

    @skip("Disabled because current mock_sqs doesn't support invalid queue call")
    @mock_sqs()
    def test_add_message_wrong_queue(self):
        sqs = boto3.resource('sqs')
        queue = sqs.create_queue(QueueName='default')
        queue_client = SqsQueueClient()

        with self.assertRaises(QueueDoesNotExistException):
            queue_client.add_message('invalid', 'msg', 0)
