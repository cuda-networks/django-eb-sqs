from abc import ABCMeta, abstractmethod


class BaseAutoTaskService:
    __metaclass__ = ABCMeta

    @abstractmethod
    def register_task(self, method, queue_name=None, max_retries=None):
        pass


class NoopTaskService(BaseAutoTaskService):
    def register_task(self, method, queue_name=None, max_retries=None):
        pass
