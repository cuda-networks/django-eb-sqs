from __future__ import absolute_import, unicode_literals

from unittest import TestCase

from mock import Mock

from eb_sqs import settings
from eb_sqs.decorators import task
from eb_sqs.worker.queue_client import QueueClient
from eb_sqs.worker.worker import Worker
from eb_sqs.worker.worker_exceptions import MaxRetriesReachedException
from eb_sqs.worker.worker_factory import WorkerFactory


class TestException(Exception):
    pass


@task()
def dummy_task(msg):
    return msg


@task(max_retries=100)
def retries_task(num_of_retries):
    if retries_task.retry_num < num_of_retries:
        retries_task.retry(execute_inline=True)


@task(max_retries=5)
def max_retries_task():
    max_retries_task.retry(execute_inline=True)


@task(max_retries=100)
def repeating_group_task(num_of_retries):
    if repeating_group_task.retry_num < num_of_retries:
        repeating_group_task.retry(execute_inline=True)


@task()
def exception_group_task():
    raise TestException()


@task(max_retries=100)
def exception_repeating_group_task(num_of_retries):
    if exception_repeating_group_task.retry_num == num_of_retries:
        raise TestException()
    else:
        exception_repeating_group_task.retry(execute_inline=True)


@task(max_retries=5)
def max_retries_group_task():
    max_retries_group_task.retry(execute_inline=True)


global_group_mock = Mock()


class WorkerTest(TestCase):
    def setUp(self):
        settings.DEAD_LETTER_MODE = False

        self.queue_mock = Mock(autospec=QueueClient)
        self.worker = Worker(self.queue_mock)

        factory_mock = Mock(autospec=WorkerFactory)
        factory_mock.create.return_value = self.worker
        settings.WORKER_FACTORY = factory_mock

    def test_worker_execution(self):
        msg = '{"id": "id-1", "retry": 0, "queue": "default", "maxRetries": 5, "args": [], "func": "eb_sqs.tests.worker.tests_worker.dummy_task", "kwargs": {"msg": "Hello World!"}}'

        result = self.worker.execute(msg, 2)

        self.assertEqual(result, 'Hello World!')

    def test_worker_execution_dead_letter_queue(self):
        settings.DEAD_LETTER_MODE = True

        msg = '{"id": "id-1", "groupId": "group-5", "retry": 0, "queue": "default", "maxRetries": 5, "args": [], "func": "eb_sqs.tests.worker.tests_worker.dummy_task", "kwargs": {"msg": "Hello World!"}}'

        result = self.worker.execute(msg)

        self.assertIsNone(result)

    def test_delay(self):
        self.worker.delay(None, 'queue', dummy_task, [], {'msg': 'Hello World!'}, 5, False, 3, False)

        self.queue_mock.add_message.assert_called_once()
        queue_delay = self.queue_mock.add_message.call_args[0][2]
        self.assertEqual(queue_delay, 3)

    def test_delay_inline(self):
        result = self.worker.delay(None, 'queue', dummy_task, [], {'msg': 'Hello World!'}, 5, False, 0, True)

        self.queue_mock.add_message.assert_not_called()
        self.assertEqual(result, 'Hello World!')

    def test_retry_max_reached_execution(self):
        with self.assertRaises(MaxRetriesReachedException):
            max_retries_task.delay(execute_inline=True)

    def test_retry_no_limit(self):
        retries_task.delay(10, execute_inline=True)

        self.assertEqual(retries_task.retry_num, 10)
