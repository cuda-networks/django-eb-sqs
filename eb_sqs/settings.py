from __future__ import absolute_import, unicode_literals

from django.conf import settings


AUTO_ADD_QUEUE = getattr(settings, 'EB_SQS_AUTO_ADD_QUEUE', True)
QUEUE_PREFIX = getattr(settings, 'EB_SQS_QUEUE_PREFIX', 'eb-sqs-')
DEFAULT_QUEUE = getattr(settings, 'EB_SQS_DEFAULT_QUEUE', 'default')

EXECUTE_INLINE = getattr(settings, 'EB_SQS_EXECUTE_INLINE', False)
FORCE_SERIALIZATION = getattr(settings, 'EB_SQS_FORCE_SERIALIZATION', False)

DEFAULT_DELAY = getattr(settings, 'EB_SQS_DEFAULT_DELAY', 0)
DEFAULT_MAX_RETRIES = getattr(settings, 'EB_SQS_DEFAULT_MAX_RETRIES', 0)

USE_PICKLE = getattr(settings, 'EB_SQS_USE_PICKLE', False)

REDIS_CLIENT = getattr(settings, 'REDIS_CLIENT', None)
