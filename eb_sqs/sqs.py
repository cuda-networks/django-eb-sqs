from __future__ import absolute_import, unicode_literals

# Don't use pyOpenSSL in urllib3 - it causes an ``OpenSSL.SSL.Error``
# exception when we try an API call on an idled persistent connection.
# See https://github.com/boto/boto3/issues/220
try:
    from botocore.vendored.requests.packages.urllib3.contrib import pyopenssl
    pyopenssl.extract_from_urllib3()
except ImportError:
    pass

import boto3
from botocore.exceptions import ClientError
from eb_sqs.settings import QUEUE_PREFIX, AUTO_ADD_QUEUE


class SqsClient:
    class QueueDoesNotExistException(Exception):
        def __init__(self, queue_name):
            # type: (unicode) -> None
            super(SqsClient.QueueDoesNotExistException, self).__init__()
            self.queue_name = queue_name

    def __init__(self):
        # type: () -> None
        self.sqs = boto3.resource('sqs')
        self.queue_cache = {}

    def _get_queue(self, queue_name):
        # type: (unicode) -> Any
        queue_name = '{}{}'.format(QUEUE_PREFIX, queue_name)

        queue = self._get_sqs_queue(queue_name)
        if not queue:
            queue = self._add_sqs_queue(queue_name)

        return queue

    def _get_sqs_queue(self, queue_name):
        # type: (unicode) -> Any
        if self.queue_cache.get(queue_name):
            return self.queue_cache[queue_name]

        try:
            queue = self.sqs.get_queue_by_name(QueueName=queue_name)
            self.queue_cache[queue_name] = queue
            return queue
        except ClientError as ex:
            error_code = ex.response.get('Error', {}).get('Code', None)
            if error_code == 'AWS.SimpleQueueService.NonExistentQueue':
                return None
            else:
                raise ex

    def _add_sqs_queue(self, queue_name):
        # type: (unicode) -> Any
        if AUTO_ADD_QUEUE:
            queue = self.sqs.create_queue(QueueName=queue_name)
            self.queue_cache[queue_name] = queue
            return queue
        else:
            raise SqsClient.QueueDoesNotExistException(queue_name)

    def add_message(self, queue_name, msg, delay):
        # type: (unicode, unicode, int) -> None
        queue = self._get_queue(queue_name)
        queue.send_message(
            MessageBody=msg,
            DelaySeconds=delay
        )
