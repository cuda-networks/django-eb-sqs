import boto3
from django.test import TestCase, Client
from moto import mock_sqs
import time

from eb_sqs.decorators import task
from eb_sqs.worker import WorkerTask, Worker


def dummy_function():
    pass


@task()
def dummy_task(msg):
    return msg


@task()
def dummy_task_with_exception():
    raise Exception()


class SerializationTest(TestCase):
    def setUp(self):
        self.dummy_msg = '{"args": [], "func": "eb_sqs.tests.dummy_function", "kwargs": {}}'

    def test_serialize_worker_task(self):
        worker_task = WorkerTask(dummy_function, [], {})
        msg = worker_task.serialize()

        self.assertEqual(msg, self.dummy_msg)

    def test_deserialize_worker_task(self):
        worker_task = WorkerTask.deserialize(self.dummy_msg)

        self.assertEqual(worker_task.func, dummy_function)
        self.assertEqual(worker_task.args, [])
        self.assertEqual(worker_task.kwargs, {})

    def test_deserialize_worker_task_missing_params(self):
        dummy_msg = '{"func": "eb_sqs.tests.dummy_function"}'
        worker_task = WorkerTask.deserialize(dummy_msg)

        self.assertEqual(worker_task.func, dummy_function)
        self.assertEqual(worker_task.args, [])
        self.assertEqual(worker_task.kwargs, {})


class TaskExecutionTest(TestCase):
    def test_inline_execution(self):
        result = dummy_task.delay('Hello World!', execute_inline=True)

        self.assertEqual(result, 'Hello World!')

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
        msg = '{"args": [], "func": "eb_sqs.tests.dummy_task", "kwargs": {"msg": "Hello World!"}}'

        worker = Worker()
        result = worker.execute(msg)

        self.assertEqual(result, 'Hello World!')


class ApiTest(TestCase):
    def test_process_endpoint(self):
        client = Client()
        msg = '{"args": [], "func": "eb_sqs.tests.dummy_task", "kwargs": {"msg": "Hello World!"}}'

        response = client.post('/process', content_type='application/json', data=msg)

        self.assertEqual(response.status_code, 200)

    def test_process_endpoint_invalid_format(self):
        client = Client()
        msg = '{ "key": "value"}'

        response = client.post('/process', content_type='application/json', data=msg)

        self.assertEqual(response.status_code, 200)

    def test_process_endpoint_invalid_function(self):
        client = Client()
        msg = '{"func": "eb_sqs.tests.dummy_task_with_exception"}'

        response = client.post('/process', content_type='application/json', data=msg)

        self.assertEqual(response.status_code, 500)
