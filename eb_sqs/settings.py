from __future__ import absolute_import, unicode_literals

from django.conf import settings

AUTO_ADD_QUEUE = getattr(settings, 'EB_SQS_AUTO_ADD_QUEUE', True) # type: bool
QUEUE_PREFIX = getattr(settings, 'EB_SQS_QUEUE_PREFIX', 'eb-sqs-') # type: unicode
DEFAULT_QUEUE = getattr(settings, 'EB_SQS_DEFAULT_QUEUE', 'default') # type: unicode

EXECUTE_INLINE = getattr(settings, 'EB_SQS_EXECUTE_INLINE', False) # type: bool
FORCE_SERIALIZATION = getattr(settings, 'EB_SQS_FORCE_SERIALIZATION', False) # type: bool

DEFAULT_DELAY = getattr(settings, 'EB_SQS_DEFAULT_DELAY', 0) # type: int
DEFAULT_MAX_RETRIES = getattr(settings, 'EB_SQS_DEFAULT_MAX_RETRIES', 0) # type: int
DEFAULT_COUNT_RETRIES = getattr(settings, 'EB_SQS_DEFAULT_COUNT_RETRIES', True) # type: bool

USE_PICKLE = getattr(settings, 'EB_SQS_USE_PICKLE', False) # type: bool

GROUP_CALLBACK_TASK = getattr(settings, 'EB_SQS_GROUP_CALLBACK_TASK', None) # type: Any

REDIS_CLIENT = getattr(settings, 'EB_SQS_REDIS_CLIENT', None) # type: StrictRedis
# default: 7 days
REDIS_EXPIRY = getattr(settings, 'EB_SQS_REDIS_EXPIRY', 3600*24*7) # type: int

WORKER_FACTORY = getattr(settings, 'EB_SQS_WORKER_FACTORY', None) # type: WorkerFactory

DEAD_LETTER_MODE = getattr(settings, 'EB_SQS_DEAD_LETTER_MODE', False) # type: bool
