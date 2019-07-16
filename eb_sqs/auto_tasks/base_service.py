from abc import ABCMeta, abstractmethod


class BaseAutoTaskService:
    __metaclass__ = ABCMeta

    @abstractmethod
    def register_task(self, method, queue_name=None, max_retries=None):
        # type: (Any, str, int) -> None
        pass


class NoopTaskService(BaseAutoTaskService):
    def __init__(self):
        # type: () -> None
        self._registered_func_names = []

    def register_task(self, method, queue_name=None, max_retries=None):
        # type: (Any, str, int) -> None
        self._registered_func_names.append(method.func_name)

    def is_func_name_registered(self, func_name):
        # type: (str) -> bool
        return func_name in self._registered_func_names
