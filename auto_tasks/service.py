import logging

from auto_tasks.base_service import BaseAutoTaskService, NoopTaskService
from auto_tasks.exceptions import RetryableTaskException
from eb_sqs.decorators import task
from eb_sqs.worker.worker_exceptions import MaxRetriesReachedException

logger = logging.getLogger(__name__)


@task()
def _auto_task_wrapper(module_name, class_name, func_name, *args, **kwargs):
    try:
        module = __import__(module_name)  # import module
        class_ = getattr(module, class_name)  # find class
        instance = class_(auto_task_service=NoopTaskService())  # instantiate class using empty AutoTaskService
        getattr(instance, func_name)(*args, **kwargs)  # invoke method on instance
    except RetryableTaskException as exc:
        try:
            _auto_task_wrapper.retry()
        except MaxRetriesReachedException:
            logger.error('Reached max retries in auto task {}.{}.{} with error: {}'.format(module_name, class_name, func_name, repr(exc)))


class AutoTaskService(BaseAutoTaskService):
    def register_task(self, method, queue_name=None, max_retries=None):
        instance = method.im_self
        class_ = instance.__class__
        func_name = method.func_name

        def _auto_task_wrapper_invoker(*args, **kwargs):
            _auto_task_wrapper.delay(
                class_.__module__,
                class_.__name__,
                func_name,
                *args, **kwargs, queue_name=queue_name, max_retries=max_retries
            )

        setattr(instance, func_name, _auto_task_wrapper_invoker)
