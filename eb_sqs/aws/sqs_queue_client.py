import re
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from eb_sqs import settings
from eb_sqs.worker.queue_client import QueueClient, QueueDoesNotExistException, QueueClientException


class SqsQueueClient(QueueClient):
    def __init__(self):
        self.sqs = boto3.resource('sqs',
                                  region_name=settings.AWS_REGION,
                                  config=Config(retries={'max_attempts': settings.AWS_MAX_RETRIES})
                                  )
        self.queue_cache = {}

    def _is_queue_url(self, queue_identifier: str) -> bool:
        """Check if the queue identifier is a full SQS URL"""
        sqs_url_pattern = r'^https://sqs\.[a-zA-Z0-9-]+\.amazonaws\.com/\d+/.+'
        return bool(re.match(sqs_url_pattern, queue_identifier))

    def _get_queue_url(self, queue_name: str) -> str:
        """Get queue URL from configuration or construct standard name"""
        # Check if there's a direct URL mapping for this queue name
        if queue_name in settings.QUEUE_URLS:
            return settings.QUEUE_URLS[queue_name]
        
        # Check if there's a cross-account configuration for this queue
        if queue_name in settings.CROSS_ACCOUNT_QUEUES:
            cross_account_config = settings.CROSS_ACCOUNT_QUEUES[queue_name]
            account_id = cross_account_config.get('account_id')
            region = cross_account_config.get('region', settings.AWS_REGION)
            actual_queue_name = cross_account_config.get('queue_name', queue_name)
            return f"https://sqs.{region}.amazonaws.com/{account_id}/{actual_queue_name}"
        
        # Return None if no special configuration - will use standard queue name
        return None

    def _get_queue(self, queue_name: str, use_cache: bool = True) -> Any:
        # Check if queue_name is already a full URL
        if self._is_queue_url(queue_name):
            return self._get_sqs_queue_by_url(queue_name, use_cache)
        
        # Get configured URL for this queue
        queue_url = self._get_queue_url(queue_name)
        if queue_url:
            return self._get_sqs_queue_by_url(queue_url, use_cache)
        
        # Use standard queue name logic for same-account queues
        full_queue_name = '{}{}'.format(settings.QUEUE_PREFIX, queue_name)
        queue = self._get_sqs_queue_by_name(full_queue_name, use_cache)
        if not queue:
            queue = self._add_sqs_queue(full_queue_name)
        
        return queue

    def _get_sqs_queue_by_url(self, queue_url: str, use_cache: bool) -> Any:
        """Get queue using full URL (supports cross-account queues)"""
        cache_key = f"url:{queue_url}"
        
        if use_cache and self.queue_cache.get(cache_key):
            return self.queue_cache[cache_key]

        try:
            # Use Queue constructor with URL for cross-account support
            queue = self.sqs.Queue(queue_url)
            
            # Verify queue exists by accessing its attributes
            _ = queue.attributes
            
            self.queue_cache[cache_key] = queue
            return queue
        except ClientError as ex:
            error_code = ex.response.get('Error', {}).get('Code', None)
            if error_code in ['AWS.SimpleQueueService.NonExistentQueue', 'QueueDoesNotExist']:
                return None
            else:
                raise ex

    def _get_sqs_queue_by_name(self, queue_name: str, use_cache: bool) -> Any:
        """Get queue by name (same-account only)"""
        if use_cache and self.queue_cache.get(queue_name):
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

    def _add_sqs_queue(self, queue_name: str) -> Any:
        """Create a new queue (same-account only)"""
        if settings.AUTO_ADD_QUEUE:
            queue = self.sqs.create_queue(
                QueueName=queue_name,
                Attributes={
                    'MessageRetentionPeriod': settings.QUEUE_MESSAGE_RETENTION,
                    'VisibilityTimeout': settings.QUEUE_VISIBILITY_TIMEOUT
                }
            )
            self.queue_cache[queue_name] = queue
            return queue
        else:
            raise QueueDoesNotExistException(queue_name)

    def add_message(self, queue_name: str, msg: str, delay: int):
        try:
            queue = self._get_queue(queue_name)
            try:
                queue.send_message(
                    MessageBody=msg,
                    DelaySeconds=delay
                )
            except ClientError as ex:
                error_code = ex.response.get('Error', {}).get('Code', None)
                if error_code in ['AWS.SimpleQueueService.NonExistentQueue', 'QueueDoesNotExist']:
                    queue = self._get_queue(queue_name, use_cache=False)
                    queue.send_message(
                        MessageBody=msg,
                        DelaySeconds=delay
                    )
                else:
                    raise ex
        except QueueDoesNotExistException:
            raise
        except Exception as ex:
            raise QueueClientException(ex)
