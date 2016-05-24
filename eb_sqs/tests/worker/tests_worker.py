from __future__ import absolute_import, unicode_literals

from unittest import TestCase

from mock import Mock

from eb_sqs import settings
from eb_sqs.decorators import task
from eb_sqs.worker.group_client import GroupClient
from eb_sqs.worker.queue_client import QueueClient
from eb_sqs.worker.worker import Worker
from eb_sqs.worker.worker_exceptions import MaxRetriesReachedException
from eb_sqs.worker.worker_factory import WorkerFactory
from eb_sqs.worker.worker_task import WorkerTask


@task()
def dummy_task(msg):
    return msg

global_group_mock = Mock()

class WorkerTest(TestCase):
    def setUp(self):
        settings.DEAD_LETTER_MODE = False

        self.queue_mock = Mock(autospec=QueueClient)
        self.group_mock = Mock(autospec=GroupClient)
        self.group_mock.remove.return_value = True
        self.worker = Worker(self.queue_mock, self.group_mock)

        factory_mock = Mock(autospec=WorkerFactory)
        factory_mock.create.return_value = self.worker
        settings.WORKER_FACTORY = factory_mock

    def test_worker_execution_no_group(self):
        msg = '{"id": "id-1", "retry": 0, "queue": "default", "maxRetries": 5, "args": [], "func": "eb_sqs.tests.worker.tests_worker.dummy_task", "kwargs": {"msg": "Hello World!"}}'

        result = self.worker.execute(msg)

        self.assertEqual(result, 'Hello World!')
        self.group_mock.remove.assert_not_called()

    def test_worker_execution_with_group(self):
        msg = '{"id": "id-1", "groupId": "group-5", "retry": 0, "queue": "default", "maxRetries": 5, "args": [], "func": "eb_sqs.tests.worker.tests_worker.dummy_task", "kwargs": {"msg": "Hello World!"}}'

        result = self.worker.execute(msg)

        self.assertEqual(result, 'Hello World!')
        self.group_mock.remove.assert_called_once()

    def test_worker_execution_dead_letter_queue(self):
        settings.DEAD_LETTER_MODE = True

        msg = '{"id": "id-1", "groupId": "group-5", "retry": 0, "queue": "default", "maxRetries": 5, "args": [], "func": "eb_sqs.tests.worker.tests_worker.dummy_task", "kwargs": {"msg": "Hello World!"}}'

        result = self.worker.execute(msg)

        self.assertIsNone(result)
        self.group_mock.remove.assert_called_once()

    def test_delay(self):
        self.worker.delay(None, 'queue', dummy_task, [], {'msg': 'Hello World!'}, 5, False, 3, False)

        self.group_mock.add.assert_not_called()
        self.queue_mock.add_message.assert_called_once()
        queue_delay = self.queue_mock.add_message.call_args[0][2]
        self.assertEqual(queue_delay, 3)

    def test_delay_inline(self):
        result = self.worker.delay(None, 'queue', dummy_task, [], {'msg': 'Hello World!'}, 5, False, 0, True)

        self.queue_mock.add_message.assert_not_called()
        self.assertEqual(result, 'Hello World!')

    def test_delay_with_group(self):
        self.worker.delay('group-id', 'queue', dummy_task, [], {'msg': 'Hello World!'}, 5, False, 3, False)

        self.group_mock.add.assert_called_once()

    def test_group_callback(self):
        settings.GROUP_CALLBACK_TASK = Mock()

        self.worker.delay('group-id', 'queue', dummy_task, [], {'msg': 'Hello World!'}, 5, False, 3, True)

        self.group_mock.remove.assert_called_once()
        settings.GROUP_CALLBACK_TASK.delay.assert_called_once()

        settings.GROUP_CALLBACK_TASK = None

    def test_group_callback_string(self):
        settings.GROUP_CALLBACK_TASK = 'eb_sqs.tests.worker.tests_worker.global_group_mock'

        self.worker.delay('group-id', 'queue', dummy_task, [], {'msg': 'Hello World!'}, 5, False, 3, True)

        self.group_mock.remove.assert_called_once()
        global_group_mock.delay.assert_called_once()

        settings.GROUP_CALLBACK_TASK = None

    def test_retry_execution(self):
        task = WorkerTask('id', None, 'queue', dummy_task, [], {'msg': 'Hello World!'}, 5, 0, False)
        self.assertEqual(dummy_task.retry_num, 0)

        self.worker.retry(task, 0, False)

        self.queue_mock.add_message.assert_called_once()

    def test_retry_max_reached_execution(self):
        with self.assertRaises(MaxRetriesReachedException):
            task = WorkerTask('id', None, 'queue', dummy_task, [], {'msg': 'Hello World!'}, 2, 0, False)
            self.assertEqual(dummy_task.retry_num, 0)

            self.worker.retry(task, 0, True)
            self.assertEqual(dummy_task.retry_num, 1)

            self.worker.retry(task, 0, True)
            self.assertEqual(dummy_task.retry_num, 2)

            self.worker.retry(task, 0, True)
