from __future__ import absolute_import, unicode_literals

from abc import ABCMeta, abstractmethod


class QueueClientException(Exception):
    pass


class QueueDoesNotExistException(QueueClientException):
    def __init__(self, queue_name):
        # type: (unicode) -> None
        super(QueueDoesNotExistException, self).__init__()
        self.queue_name = queue_name


class QueueClient(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def add_message(self, queue_name, msg, delay):
        # type: (unicode, unicode, int) -> None
        pass
