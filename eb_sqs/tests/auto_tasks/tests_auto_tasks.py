from unittest import TestCase

from mock import Mock

from eb_sqs.auto_tasks.service import AutoTaskService, _auto_task_wrapper


class TestService:
    _TEST_MOCK = Mock()

    def __init__(self, auto_task_service=None):
        self._auto_task_service = auto_task_service or AutoTaskService()
        self._auto_task_service.register_task(self.task_method)

    def task_method(self, *args, **kwargs):
        self._TEST_MOCK.task_method(*args, **kwargs)

    def non_task_method(self):
        self._TEST_MOCK.non_task_method()


class AutoTasksTest(TestCase):
    def setUp(self):
        self._test_service = TestService()

    def test_task_method(self):
        args = [5, '6']
        kwargs = {'p1': 'bla', 'p2': 130}

        self._test_service.task_method(*args, **dict(kwargs, execute_inline=True))

        TestService._TEST_MOCK.task_method.assert_called_once_with(*args, **kwargs)

    def test_non_task_method(self):
        _auto_task_wrapper.delay(
            self._test_service.__class__.__module__,
            self._test_service.__class__.__name__,
            TestService.non_task_method.__name__,
            execute_inline=True
        )

        TestService._TEST_MOCK.non_task_method.assert_not_called()
