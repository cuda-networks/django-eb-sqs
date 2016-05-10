from __future__ import absolute_import, unicode_literals

from unittest import TestCase

from django.test import Client

from eb_sqs.decorators import task


@task()
def dummy_task(msg):
    return msg


@task()
def dummy_task_with_exception():
    # type: () -> None
    raise Exception()


class ApiTest(TestCase):
    def test_process_endpoint(self):
        client = Client()
        msg = '{"retry": 0, "queue": "default", "max_retries": 5, "args": [], "func": "eb_sqs.tests.tests_views.dummy_task", "kwargs": {"msg": "Hello World!"}}'

        response = client.post('/process', content_type='application/json', data=msg)

        self.assertEqual(response.status_code, 200)

    def test_process_endpoint_invalid_format(self):
        client = Client()
        msg = '{ "key": "value"}'

        response = client.post('/process', content_type='application/json', data=msg)

        self.assertEqual(response.status_code, 400)

    def test_process_endpoint_invalid_function(self):
        client = Client()
        msg = '{"retry": 0, "queue": "default", "max_retries": 5, "args": [], "func": "eb_sqs.tests.tests_views.dummy_task_with_exception", "kwargs": {}, "pickle": false}'

        response = client.post('/process', content_type='application/json', data=msg)

        self.assertEqual(response.status_code, 500)
