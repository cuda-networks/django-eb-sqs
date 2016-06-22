from __future__ import absolute_import, unicode_literals

from redis import StrictRedis

from eb_sqs import settings
from eb_sqs.worker.group_client import GroupClient
from eb_sqs.worker.worker_task import WorkerTask


class RedisGroupClient(GroupClient):
    def __init__(self, redis_client):
        # type: (StrictRedis) -> None
        super(RedisGroupClient, self).__init__()
        self._redis_client = redis_client

    def _key_name(self, group_id):
        # type: (unicode) -> None
        return '{}{}'.format(settings.REDIS_KEY_PREFIX, group_id)

    def add(self, worker_task):
        # type: (WorkerTask) -> None
        name = self._key_name(worker_task.group_id)
        pipe = self._redis_client.pipeline()
        pipe.sadd(name, worker_task.id)\
            .expire(name, settings.REDIS_EXPIRY)\
            .execute()

    def remove(self, worker_task):
        # type: (WorkerTask) -> bool
        """
        :return: True if last task in group
        """
        name = self._key_name(worker_task.group_id)
        if self._redis_client.srem(name, worker_task.id) > 0:
            return self._redis_client.scard(name) == 0
        else:
            return False

    def active_tasks(self, group_id):
        # type: (unicode) -> int
        name = self._key_name(group_id)
        return self._redis_client.scard(name)
