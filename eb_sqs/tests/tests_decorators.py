from __future__ import absolute_import, unicode_literals

from unittest import TestCase

from mock import Mock

from eb_sqs import settings
from eb_sqs.decorators import task
from eb_sqs.worker.worker import Worker
from eb_sqs.worker.worker_factory import WorkerFactory


@task()
def dummy_task(msg):
    # type: (unicode) -> None
    if not msg:
        raise Exception('No message')

@task(queue_name='CustomQueue')
def dummy_task_custom_queue():
    # type: (unicode) -> None
    pass

@task()
def dummy_retry_task(msg):
    # type: (unicode) -> None
    if dummy_retry_task.retry_num == 0:
        dummy_retry_task.retry()
    else:
        if not msg:
            raise Exception('No message')


class DecoratorsTest(TestCase):
    def setUp(self):
        self.worker_mock = Mock(autospec=Worker)

        factory_mock = Mock(autospec=WorkerFactory)
        factory_mock.create.return_value = self.worker_mock
        settings.WORKER_FACTORY = factory_mock

    def test_delay_decorator(self):
        dummy_task.delay('Hello World!')
        self.worker_mock.delay.assert_called_once()

    def test_delay_custom_queue_decorator(self):
        dummy_task_custom_queue.delay()

        call_args = self.worker_mock.delay.call_args
        self.assertTrue('CustomQueue' in call_args[0])

    def test_delay_custom_queue_as_param_decorator(self):
        dummy_task_custom_queue.delay(queue_name='OtherQueue')

        call_args = self.worker_mock.delay.call_args
        self.assertTrue('OtherQueue' in call_args[0])

    def test_delay_decorator_parameters(self):
        dummy_task.delay('Hello World!', group_id='group-5', delay=5, execute_inline=True)

        self.worker_mock.delay.assert_called_once()

        # Parameter should have been removed from kwargs before being passed to the actual function
        kwargs = self.worker_mock.delay.call_args[0][4]
        self.assertEqual(kwargs, {})

    def test_retry_decorator(self):
        dummy_retry_task.delay('Hello World!')
        self.worker_mock.delay.assert_called_once()
