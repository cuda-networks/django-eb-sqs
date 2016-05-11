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

    def add(self, worker_task):
        # type: (WorkerTask) -> None
        pipe = self._redis_client.pipeline()
        pipe.sadd(worker_task.group_id, worker_task.id)\
            .expire(worker_task.group_id, settings.REDIS_EXPIRY)\
            .execute()

    def remove(self, worker_task):
        # type: (WorkerTask) -> bool
        """
        :return: True if last task in group
        """
        return self._redis_client.srem(worker_task.group_id, worker_task.id) > 0

    def active_tasks(self, group_id):
        # type: (unicode) -> int
        return self._redis_client.scard(group_id)
