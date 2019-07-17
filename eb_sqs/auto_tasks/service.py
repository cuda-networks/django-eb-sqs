import importlib
import logging

from eb_sqs.auto_tasks.base_service import BaseAutoTaskService, NoopTaskService
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

        noop_task_service = NoopTaskService()
        instance = class_(auto_task_service=noop_task_service)  # instantiate class using NoopTaskService

        if noop_task_service.is_func_name_registered(func_name):
            getattr(instance, func_name)(*args, **kwargs)  # invoke method on instance
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


class AutoTaskService(BaseAutoTaskService):
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
