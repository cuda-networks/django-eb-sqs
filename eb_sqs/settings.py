from django.conf import settings


AUTO_ADD_QUEUE = getattr(settings, 'EB_SQS_AUTO_ADD_QUEUE', True)
QUEUE_PREFIX = getattr(settings, 'EB_SQS_QUEUE_PREFIX', 'eb-sqs-')
DEFAULT_QUEUE = getattr(settings, 'EB_SQS_DEFAULT_QUEUE', 'default')

EXECUTE_INLINE = getattr(settings, 'EB_SQS_EXECUTE_INLINE', False)

DEFAULT_DELAY = getattr(settings, 'EB_SQS_DEFAULT_DELAY', 0)
DEFAULT_MAX_RETRIES = getattr(settings, 'EB_SQS_DEFAULT_MAX_RETRIES', 0)
