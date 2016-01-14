import boto3
from django.test import TestCase, Client
from moto import mock_sqs
import time

from eb_sqs.decorators import task, MaxRetriesReachedException
from eb_sqs.worker import WorkerTask, Worker


def dummy_function():
    pass


@task()
def dummy_task(msg):
    return msg


@task(max_retries=5)
def dummy_retry_task(msg):
    dummy_retry_task.retry(execute_inline=True)


@task()
def dummy_task_with_exception():
    raise Exception()


class SerializationTest(TestCase):
    def setUp(self):
        self.dummy_msg = '{"retry": 0, "args": [], "queue": "default", "max_retries": 5, "func": "eb_sqs.tests.dummy_function", "kwargs": {}}'

    def test_serialize_worker_task(self):
        worker_task = WorkerTask('default', dummy_function, [], {}, 5, 0)
        msg = worker_task.serialize()

        self.assertEqual(msg, self.dummy_msg)

    def test_deserialize_worker_task(self):
        worker_task = WorkerTask.deserialize(self.dummy_msg)

        self.assertEqual(worker_task.queue, 'default')
        self.assertEqual(worker_task.func, dummy_function)
        self.assertEqual(worker_task.args, [])
        self.assertEqual(worker_task.kwargs, {})
        self.assertEqual(worker_task.max_retries, 5)
        self.assertEqual(worker_task.retry, 0)

    def test_deserialize_worker_task_missing_params(self):
        dummy_msg = '{"retry": 0, "queue": "default", "max_retries": 3, "func": "eb_sqs.tests.dummy_function"}'
        worker_task = WorkerTask.deserialize(dummy_msg)

        self.assertEqual(worker_task.queue, 'default')
        self.assertEqual(worker_task.func, dummy_function)
        self.assertEqual(worker_task.args, [])
        self.assertEqual(worker_task.kwargs, {})
        self.assertEqual(worker_task.max_retries, 3)
        self.assertEqual(worker_task.retry, 0)


class TaskExecutionTest(TestCase):
    def test_inline_execution(self):
        result = dummy_task.delay('Hello World!', execute_inline=True)

        self.assertEqual(result, 'Hello World!')

    def test_inline_retry_execution(self):
        with self.assertRaises(MaxRetriesReachedException):
            dummy_retry_task.delay('Hello World!', execute_inline=True)

    @mock_sqs()
    def test_delay_0_execution(self):
        sqs = boto3.resource('sqs')
        queue = sqs.create_queue(QueueName='eb-sqs-default')

        dummy_task.delay('Hello World!')

        queue.reload()
        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '1')

    @mock_sqs()
    def test_delay_1_execution(self):
        delay = 1
        sqs = boto3.resource('sqs')
        queue = sqs.create_queue(QueueName='eb-sqs-default')

        dummy_task.delay('Hello World!', delay=delay)

        queue.reload()
        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '0')

        time.sleep(delay+0.1)

        queue.reload()
        self.assertEqual(queue.attributes["ApproximateNumberOfMessages"], '1')

    def test_worker_execution(self):
        msg = '{"retry": 0, "queue": "default", "max_retries": 5, "args": [], "func": "eb_sqs.tests.dummy_task", "kwargs": {"msg": "Hello World!"}}'

        worker = Worker()
        result = worker.execute(msg)

        self.assertEqual(result, 'Hello World!')


class ApiTest(TestCase):
    def test_process_endpoint(self):
        client = Client()
        msg = '{"retry": 0, "queue": "default", "max_retries": 5, "args": [], "func": "eb_sqs.tests.dummy_task", "kwargs": {"msg": "Hello World!"}}'

        response = client.post('/process', content_type='application/json', data=msg)

        self.assertEqual(response.status_code, 200)

    def test_process_endpoint_invalid_format(self):
        client = Client()
        msg = '{ "key": "value"}'

        response = client.post('/process', content_type='application/json', data=msg)

        self.assertEqual(response.status_code, 400)

    def test_process_endpoint_invalid_function(self):
        client = Client()
        msg = '{"retry": 0, "queue": "default", "max_retries": 5, "func": "eb_sqs.tests.dummy_task_with_exception"}'

        response = client.post('/process', content_type='application/json', data=msg)

        self.assertEqual(response.status_code, 500)
