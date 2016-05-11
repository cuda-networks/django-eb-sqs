from __future__ import absolute_import, unicode_literals

from abc import ABCMeta, abstractmethod

from eb_sqs.worker.worker_task import WorkerTask


class GroupClient(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        # type: () -> None
        super(GroupClient, self).__init__()

    @abstractmethod
    def add(self, worker_task):
        # type: (WorkerTask) -> None
        pass

    @abstractmethod
    def remove(self, worker_task):
        # type: (WorkerTask) -> bool
        """
        :return: True if last task in group
        """
        pass

    @abstractmethod
    def active_tasks(self, group_id):
        # type: (unicode) -> int
        pass
