from unittest import TestCase

from mock import Mock, call

from eb_sqs import settings
from eb_sqs.auto_tasks.exceptions import RetryableTaskException
from eb_sqs.auto_tasks.service import AutoTaskService, _auto_task_wrapper


class TestService:
    TEST_MOCK = None
    MAX_RETRY_NUM = 5

    def __init__(self, auto_task_service=None):
        self._auto_task_service = auto_task_service or AutoTaskService()

        self._auto_task_service.register_task(self.task_method)
        self._auto_task_service.register_task(self.task_retry_method, max_retries=self.MAX_RETRY_NUM)

        self._auto_task_service.register_task(self.task_recursive_method)
        self._auto_task_service.register_task(self.task_other_method)

    def task_method(self, *args, **kwargs):
        self.TEST_MOCK.task_method(*args, **kwargs)

    def task_retry_method(self, *args, **kwargs):
        self.TEST_MOCK.task_retry_method(*args, **kwargs)

        def max_retry_fun():
            self.TEST_MOCK.task_max_retry_method(*args, **kwargs)

        raise RetryableTaskException(Exception('Test'), max_retries_func=max_retry_fun)

    def non_task_method(self):
        self.TEST_MOCK.non_task_method()

    def task_recursive_method(self, tries=2):
        if tries > 0:
            self.task_recursive_method(tries=tries - 1)
        else:
            self.task_other_method()

    def task_other_method(self):
        self.TEST_MOCK.task_other_method()


class AutoTasksTest(TestCase):
    def setUp(self):
        self._test_service = TestService()

        self._args = [5, '6']
        self._kwargs = {'p1': 'bla', 'p2': 130}

        settings.EXECUTE_INLINE = True

        TestService.TEST_MOCK = Mock()

    def test_task_method(self):
        self._test_service.task_method(*self._args, **self._kwargs)

        TestService.TEST_MOCK.task_method.assert_called_once_with(*self._args, **self._kwargs)

    def test_task_retry_method(self):
        self._test_service.task_retry_method(*self._args, **self._kwargs)

        TestService.TEST_MOCK.task_retry_method.assert_has_calls([call(*self._args, **self._kwargs)] * TestService.MAX_RETRY_NUM)

        TestService.TEST_MOCK.task_max_retry_method.assert_called_once_with(*self._args, **self._kwargs)

    def test_non_task_method(self):
        _auto_task_wrapper.delay(
            self._test_service.__class__.__module__,
            self._test_service.__class__.__name__,
            TestService.non_task_method.__name__,
            execute_inline=True
        )

        TestService.TEST_MOCK.non_task_method.assert_not_called()

    def test_task_recursive_method(self):
        self._test_service.task_recursive_method()

        TestService.TEST_MOCK.task_other_method.assert_called_once_with()
