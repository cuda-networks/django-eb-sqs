from __future__ import absolute_import, unicode_literals

from unittest import TestCase

from django.test import Client
from mock import Mock

from eb_sqs import settings
from eb_sqs.worker.worker import Worker
from eb_sqs.worker.worker_exceptions import InvalidMessageFormatException, ExecutionFailedException
from eb_sqs.worker.worker_factory import WorkerFactory


class ApiTest(TestCase):
    def setup_worker(self, side_effect):
        worker_mock = Mock(autospec=Worker)
        worker_mock.execute.side_effect = side_effect

        worker_factory_mock = Mock(autospec=WorkerFactory)
        worker_factory_mock.create.return_value = worker_mock

        settings.WORKER_FACTORY = worker_factory_mock

    def test_process_endpoint(self):
        self.setup_worker(None)
        client = Client()
        response = client.post('/process', content_type='application/json', data='')

        self.assertEqual(response.status_code, 200)

    def test_process_endpoint_invalid_format(self):
        self.setup_worker(InvalidMessageFormatException('', None))
        client = Client()

        response = client.post('/process', content_type='application/json', data='')

        self.assertEqual(response.status_code, 400)

    def test_process_endpoint_invalid_function(self):
        self.setup_worker(ExecutionFailedException('', None))
        client = Client()

        response = client.post('/process', content_type='application/json', data='')

        self.assertEqual(response.status_code, 500)
