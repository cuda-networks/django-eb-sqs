#!/usr/bin/env python3
"""
Test script to verify cross-account SQS functionality
"""

import os
import sys
import django
from django.conf import settings

# Configure Django settings
if not settings.configured:
    settings.configure(
        DEBUG=True,
        INSTALLED_APPS=[
            'eb_sqs',
        ],
        # Cross-account SQS settings examples
        # NOTE: These are example account IDs for testing purposes only - they do not correspond to real AWS accounts
        EB_SQS_CROSS_ACCOUNT_QUEUES={
            'external-queue': {
                'account_id': '123456789012',  # Example account ID - not a real account
                'region': 'us-west-2',
                'queue_name': 'actual-queue-name'
            },
            'prod-notifications': {
                'account_id': '987654321098',  # Example account ID - not a real account
                'queue_name': 'notification-queue'
            }
        },
        EB_SQS_QUEUE_URLS={
            'direct-url-queue': 'https://sqs.us-west-2.amazonaws.com/123456789012/direct-queue'  # Example URL - not a real queue
        },
        EB_AWS_REGION='us-east-1'
    )

django.setup()

def test_cross_account_functionality():
    """Test that the cross-account functionality works as expected"""
    
    # Import after Django setup
    from eb_sqs.aws.sqs_queue_client import SqsQueueClient
    
    # Create client
    client = SqsQueueClient()
    
    print("Testing cross-account SQS functionality...")
    
    # Test 1: URL detection
    print("\n1. Testing URL detection...")
    
    # Valid SQS URLs - testing various AWS region formats
    valid_urls = [
        'https://sqs.us-east-1.amazonaws.com/123456789012/test-queue',
        'https://sqs.eu-west-1.amazonaws.com/987654321098/another-queue',
        'https://sqs.ap-southeast-1.amazonaws.com/111222333444/complex-region-queue',
        'https://sqs.ca-central-1.amazonaws.com/555666777888/canada-queue',
        'https://sqs.us-gov-east-1.amazonaws.com/999000111222/gov-cloud-queue'
    ]
    
    for url in valid_urls:
        result = client._is_queue_url(url)
        print(f"   {url}: {result} ✓" if result else f"   {url}: {result} ✗")
        assert result, f"URL should be detected as valid: {url}"
    
    # Invalid URLs
    invalid_urls = [
        'regular-queue-name',
        'prefix:test-',
        'http://example.com/queue',
        'not-a-url'
    ]
    
    for url in invalid_urls:
        result = client._is_queue_url(url)
        print(f"   {url}: {result} ✓" if not result else f"   {url}: {result} ✗")
        assert not result, f"URL should NOT be detected as valid: {url}"
    
    # Test 2: Queue URL generation from cross-account config
    print("\n2. Testing cross-account queue URL generation...")
    
    # Test configured cross-account queue
    external_url = client._get_queue_url('external-queue')
    expected_url = 'https://sqs.us-west-2.amazonaws.com/123456789012/actual-queue-name'
    print(f"   external-queue: {external_url}")
    assert external_url == expected_url, f"Expected {expected_url}, got {external_url}"
    
    # Test queue with default region
    prod_url = client._get_queue_url('prod-notifications')
    expected_prod_url = 'https://sqs.us-east-1.amazonaws.com/987654321098/notification-queue'
    print(f"   prod-notifications: {prod_url}")
    assert prod_url == expected_prod_url, f"Expected {expected_prod_url}, got {prod_url}"
    
    # Test 3: Direct URL mapping
    print("\n3. Testing direct URL mapping...")
    
    direct_url = client._get_queue_url('direct-url-queue')
    expected_direct_url = 'https://sqs.us-west-2.amazonaws.com/123456789012/direct-queue'
    print(f"   direct-url-queue: {direct_url}")
    assert direct_url == expected_direct_url, f"Expected {expected_direct_url}, got {direct_url}"
    
    # Test 4: Regular queue (should return None)
    print("\n4. Testing regular queue handling...")
    
    regular_queue_url = client._get_queue_url('regular-queue')
    print(f"   regular-queue: {regular_queue_url}")
    assert regular_queue_url is None, "Regular queue should return None"
    
    print("\n✅ All cross-account SQS functionality tests passed!")
    
    # Test 5: Worker service queue resolution
    print("\n5. Testing worker service queue resolution...")
    
    from eb_sqs.worker.service import WorkerService
    worker_service = WorkerService()
    
    # Test URL detection in worker service
    test_urls = [
        'https://sqs.us-east-1.amazonaws.com/123456789012/test-queue',
        'regular-queue-name'
    ]
    
    for url in test_urls:
        is_url = worker_service._is_queue_url(url)
        print(f"   Worker service URL detection for '{url}': {is_url}")
        if url.startswith('https://sqs.'):
            assert is_url, f"Worker service should detect URL: {url}"
        else:
            assert not is_url, f"Worker service should NOT detect URL: {url}"
    
    print("\n✅ Worker service tests passed!")
    
    return True

if __name__ == '__main__':
    try:
        test_cross_account_functionality()
        print("\n🎉 All tests completed successfully! Cross-account SQS support is working.")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
