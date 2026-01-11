"""
Django settings example for cross-account SQS support

This file demonstrates how to configure django-eb-sqs for cross-account SQS access.
Copy the relevant settings to your Django settings.py file.
"""

# Basic django-eb-sqs settings
EB_AWS_REGION = 'us-east-1'
EB_SQS_DEFAULT_QUEUE = 'eb-sqs-default'

# Method 1: Cross-Account Queue Configuration
# This method allows you to define cross-account queues by specifying the account ID,
# region, and queue name. The library will construct the full SQS URL automatically.
EB_SQS_CROSS_ACCOUNT_QUEUES = {
    'external-notifications': {
        'account_id': '123456789012',  # Target AWS account ID
        'region': 'us-west-2',        # Optional: defaults to EB_AWS_REGION
        'queue_name': 'notification-queue'  # Optional: defaults to the key name
    },
    'prod-analytics': {
        'account_id': '987654321098',
        'queue_name': 'analytics-data-queue'  # Will use us-east-1 region (default)
    },
    'partner-events': {
        'account_id': '555666777888',
        'region': 'eu-west-1',
        'queue_name': 'partner-integration-events'
    }
}

# Method 2: Direct Queue URLs
# This method allows you to specify full SQS queue URLs directly.
# Use this when you already know the exact URLs or when queue names have special characters.
EB_SQS_QUEUE_URLS = {
    'legacy-system': 'https://sqs.us-east-1.amazonaws.com/111222333444/legacy-processing-queue',
    'fifo-queue': 'https://sqs.us-west-2.amazonaws.com/123456789012/high-priority.fifo',
    'dev-testing': 'https://sqs.eu-central-1.amazonaws.com/999888777666/development-test-queue'
}

# You can mix both methods - the library will check QUEUE_URLS first, then CROSS_ACCOUNT_QUEUES
# For example:
# - 'external-notifications' will use cross-account config (method 1)
# - 'legacy-system' will use direct URL (method 2)
# - 'regular-queue' will use standard same-account behavior

# Other standard django-eb-sqs settings remain the same
EB_SQS_AUTO_ADD_QUEUE = False  # Cannot auto-create cross-account queues
EB_SQS_MAX_NUMBER_OF_MESSAGES = 10
EB_SQS_WAIT_TIME_S = 2
EB_SQS_DEFAULT_DELAY = 0
EB_SQS_DEFAULT_MAX_RETRIES = 3

# Example usage in your Django application:

"""
from eb_sqs.decorators import task

@task(queue_name='external-notifications')
def send_notification_to_external_system(data):
    # This task will be sent to the cross-account queue
    # https://sqs.us-west-2.amazonaws.com/123456789012/notification-queue
    print(f"Processing notification: {data}")

@task(queue_name='legacy-system')
def process_legacy_data(payload):
    # This task will be sent to the queue specified in QUEUE_URLS
    print(f"Processing legacy data: {payload}")

@task(queue_name='regular-queue')
def normal_task(message):
    # This will use standard same-account behavior
    # Queue name will be prefixed if EB_SQS_QUEUE_PREFIX is set
    print(f"Processing regular task: {message}")

# Send tasks to cross-account queues
send_notification_to_external_system.delay(data={'event': 'user_registered'})
process_legacy_data.delay(payload={'id': 12345, 'type': 'import'})
normal_task.delay(message='Hello World')
"""

# Worker command examples:
"""
# Process cross-account queues:
python manage.py process_queue --queues external-notifications,prod-analytics

# Process mix of cross-account and regular queues:
python manage.py process_queue --queues external-notifications,regular-queue,legacy-system

# Process using full URLs directly:
python manage.py process_queue --queues https://sqs.us-west-2.amazonaws.com/123456789012/notification-queue

# Use prefixes (only works with same-account queues):
python manage.py process_queue --queues prefix:dev-,external-notifications
"""
