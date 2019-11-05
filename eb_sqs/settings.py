from __future__ import absolute_import, unicode_literals

from django.conf import settings

AWS_REGION = getattr(settings, 'EB_AWS_REGION', 'us-east-1')  # type: str

MAX_NUMBER_OF_MESSAGES = getattr(settings, 'EB_SQS_MAX_NUMBER_OF_MESSAGES', 10)  # type: int
WAIT_TIME_S = getattr(settings, 'EB_SQS_WAIT_TIME_S', 2)  # type: int
NO_QUEUES_WAIT_TIME_S = getattr(settings, 'NO_QUEUES_WAIT_TIME_S', 5)  # type: int

AUTO_ADD_QUEUE = getattr(settings, 'EB_SQS_AUTO_ADD_QUEUE', False)  # type: bool
QUEUE_PREFIX = getattr(settings, 'EB_SQS_QUEUE_PREFIX', 'eb-sqs-')  # type: str
DEFAULT_QUEUE = getattr(settings, 'EB_SQS_DEFAULT_QUEUE', 'default')  # type: str

EXECUTE_INLINE = getattr(settings, 'EB_SQS_EXECUTE_INLINE', False)  # type: bool
FORCE_SERIALIZATION = getattr(settings, 'EB_SQS_FORCE_SERIALIZATION', False)  # type: bool

DEFAULT_DELAY = getattr(settings, 'EB_SQS_DEFAULT_DELAY', 0)  # type: int
DEFAULT_MAX_RETRIES = getattr(settings, 'EB_SQS_DEFAULT_MAX_RETRIES', 0)  # type: int
DEFAULT_COUNT_RETRIES = getattr(settings, 'EB_SQS_DEFAULT_COUNT_RETRIES', True)  # type: bool

USE_PICKLE = getattr(settings, 'EB_SQS_USE_PICKLE', False)  # type: bool

GROUP_CALLBACK_TASK = getattr(settings, 'EB_SQS_GROUP_CALLBACK_TASK', None)  # type: Any

REDIS_CLIENT = getattr(settings, 'EB_SQS_REDIS_CLIENT', None)  # type: StrictRedis
# default: 7 days
REDIS_EXPIRY = getattr(settings, 'EB_SQS_REDIS_EXPIRY', 3600 * 24 * 7)  # type: int
REDIS_KEY_PREFIX = getattr(settings, 'EB_SQS_REDIS_KEY_PREFIX', 'eb-sqs-')  # type: str

WORKER_FACTORY = getattr(settings, 'EB_SQS_WORKER_FACTORY', None)  # type: WorkerFactory

DEAD_LETTER_MODE = getattr(settings, 'EB_SQS_DEAD_LETTER_MODE', False)  # type: bool

AWS_MAX_RETRIES = getattr(settings, 'EB_SQS_AWS_MAX_RETRIES', 30)  # type: int

REFRESH_PREFIX_QUEUES_S = getattr(settings, 'EB_SQS_REFRESH_PREFIX_QUEUES_S', 10)  # type: int

QUEUE_MESSAGE_RETENTION = getattr(settings, 'EB_SQS_QUEUE_MESSAGE_RETENTION', '1209600')  # type: str
QUEUE_VISIBILITY_TIMEOUT = getattr(settings, 'EB_SQS_QUEUE_VISIBILITY_TIMEOUT', '300')  # type: str

MIN_HEALTHCHECK_WRITE_PERIOD_S = getattr(settings, 'EB_SQS_MIN_HEALTHCHECK_WRITE_PERIOD_S', 10)  # type: int
HEALTHCHECK_UNHEALTHY_PERIOD_S = getattr(settings, 'EB_SQS_HEALTHCHECK_UNHEALTHY_PERIOD_S', int(QUEUE_VISIBILITY_TIMEOUT))  # type: int
HEALTHCHECK_FILE_NAME = getattr(settings, 'EB_SQS_HEALTHCHECK_FILE_NAME', 'healthcheck.txt')  # type: str
