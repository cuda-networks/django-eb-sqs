from __future__ import absolute_import, unicode_literals

from unittest import TestCase

import boto3
import time
from moto import mock_sqs

from eb_sqs.aws.sqs_queue_client import SqsQueueClient
from eb_sqs.decorators import task
from eb_sqs.worker.worker import Worker
from eb_sqs.worker.worker_exceptions import MaxRetriesReachedException


@task()
def dummy_task(msg):
    return msg


@task(max_retries=5)
def dummy_retry_inline_task(msg):
    # type: (unicode) -> None
    dummy_retry_inline_task.retry(execute_inline=True)


@task(max_retries=5)
def dummy_retry_task(msg):
    # type: (unicode) -> None
    if dummy_retry_task.retry_num == 0:
        dummy_retry_task.retry()


class TaskExecutionTest(TestCase):
    def test_inline_execution(self):
        result = dummy_task.delay('Hello World!', execute_inline=True)

        self.assertEqual(result, 'Hello World!')

    def test_inline_retry_execution(self):
        with self.assertRaises(MaxRetriesReachedException):
            dummy_retry_inline_task.delay('Hello World!', execute_inline=True)

    @mock_sqs()
    def test_delay_0_execution(self):
        sqs = boto3.resource('sqs')
        queue = sqs.create_queue(QueueName='eb-sqs-default')

        dummy_task.delay('Hello World!')

        queue.reload()
        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '1')

    @mock_sqs()
    def test_delay_1_execution(self):
        delay = 1
        sqs = boto3.resource('sqs')
        queue = sqs.create_queue(QueueName='eb-sqs-default')

        dummy_task.delay('Hello World!', delay=delay)

        queue.reload()
        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '0')

        time.sleep(delay+0.1)

        queue.reload()
        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '1')

    @mock_sqs()
    def test_retry_execution(self):
        sqs = boto3.resource('sqs')
        queue = sqs.create_queue(QueueName='eb-sqs-default')
        worker = Worker(SqsQueueClient.get_instance())

        dummy_retry_task.delay('Hello World!')
        self.assertEqual(dummy_retry_task.retry_num, 0)

        messages = queue.receive_messages()
        self.assertEqual(len(messages), 1)
        queue.reload()

        # Execute task (calls retry inside the task)
        worker.execute(messages[0].body)
        self.assertEqual(dummy_retry_task.retry_num, 0)

        messages = queue.receive_messages()
        self.assertEqual(len(messages), 1)
        queue.reload()

        # Execute retry task
        worker.execute(messages[0].body)
        self.assertEqual(dummy_retry_task.retry_num, 1)

        queue.reload()
        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '0')

    @mock_sqs()
    def test_worker_execution(self):
        msg = '{"retry": 0, "queue": "default", "max_retries": 5, "args": [], "func": "eb_sqs.tests.worker.tests_worker.dummy_task", "kwargs": {"msg": "Hello World!"}}'

        worker = Worker(SqsQueueClient.get_instance())
        result = worker.execute(msg)

        self.assertEqual(result, 'Hello World!')
