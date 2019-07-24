import importlib
import logging

from eb_sqs.auto_tasks.exceptions import RetryableTaskException
from eb_sqs.decorators import task
from eb_sqs.worker.worker_exceptions import MaxRetriesReachedException

logger = logging.getLogger(__name__)


@task()
def _auto_task_wrapper(module_name, class_name, func_name, *args, **kwargs):
    try:
        logger.debug(
            'Invoke _auto_task_wrapper with module: %s class: %s func: %s args: %s and kwargs: %s',
            module_name,
            class_name,
            func_name,
            args,
            kwargs
        )

        module = importlib.import_module(module_name)  # import module
        class_ = getattr(module, class_name)  # find class

        auto_task_executor_service = _AutoTaskExecutorService(func_name)
        instance = class_(auto_task_service=auto_task_executor_service)  # instantiate class using _AutoTaskExecutorService

        executor_func_name = auto_task_executor_service.get_executor_func_name()
        if executor_func_name:
            getattr(instance, executor_func_name)(*args, **kwargs)  # invoke method on instance
        else:
            logger.error(
                'Trying to invoke _auto_task_wrapper for unregistered task with module: %s class: %s func: %s args: %s and kwargs: %s',
                module_name,
                class_name,
                func_name,
                args,
                kwargs
            )
    except RetryableTaskException as exc:
        try:
            retry_kwargs = {}

            if exc.delay is not None:
                retry_kwargs['delay'] = exc.delay

            if exc.count_retries is not None:
                retry_kwargs['count_retries'] = exc.count_retries

            _auto_task_wrapper.retry(**retry_kwargs)
        except MaxRetriesReachedException:
            if exc.max_retries_func:
                exc.max_retries_func()
            else:
                # by default log an error
                logger.error('Reached max retries in auto task {}.{}.{} with error: {}'.format(module_name, class_name, func_name, repr(exc)))


class AutoTaskService(object):
    def register_task(self, method, queue_name=None, max_retries=None):
        # type: (Any, str, int) -> None
        instance = method.__self__
        class_ = instance.__class__
        func_name = method.__name__

        def _auto_task_wrapper_invoker(*args, **kwargs):
            if queue_name is not None:
                kwargs['queue_name'] = queue_name

            if max_retries is not None:
                kwargs['max_retries'] = max_retries

            _auto_task_wrapper.delay(
                class_.__module__,
                class_.__name__,
                func_name,
                *args, **kwargs
            )

        setattr(instance, func_name, _auto_task_wrapper_invoker)


class _AutoTaskExecutorService(AutoTaskService):
    def __init__(self, func_name):
        # type: (str) -> None
        self._func_name = func_name

        self._executor_func_name = None

    def register_task(self, method, queue_name=None, max_retries=None):
        # type: (Any, str, int) -> None
        if self._func_name == method.__name__:
            # circuit breaker to allow actually executing the method once
            instance = method.__self__

            self._executor_func_name = self._func_name + '__auto_task_executor__'
            setattr(instance, self._executor_func_name, getattr(instance, self._func_name))

        super(_AutoTaskExecutorService, self).register_task(method, queue_name, max_retries)

    def get_executor_func_name(self):
        # type: () -> str
        return self._executor_func_name
