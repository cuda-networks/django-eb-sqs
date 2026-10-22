from __future__ import absolute_import, unicode_literals

from unittest import TestCase, skipUnless

import boto3
from mock import MagicMock, patch
from moto import mock_sqs

from eb_sqs import settings, tracing
from eb_sqs.aws.sqs_queue_client import SqsQueueClient
from eb_sqs.worker.service import WorkerService

try:
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    SDK_AVAILABLE = True
except ImportError:
    SDK_AVAILABLE = False


@skipUnless(SDK_AVAILABLE, 'opentelemetry-sdk is not installed')
class TracingPropagationTest(TestCase):
    def setUp(self):
        settings.QUEUE_PREFIX = 'eb-sqs-'
        self.exporter = InMemorySpanExporter()
        self.provider = TracerProvider()
        self.provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.tracer = self.provider.get_tracer('test')

        patcher = patch.object(tracing.trace, 'get_tracer', lambda *a, **kw: self.provider.get_tracer('eb_sqs'))
        patcher.start()
        self.addCleanup(patcher.stop)

    def _spans(self, name):
        return [s for s in self.exporter.get_finished_spans() if s.name == name]

    @mock_sqs()
    def test_send_writes_traceparent_attribute(self):
        queue = boto3.resource('sqs', region_name=settings.AWS_REGION).create_queue(QueueName='eb-sqs-default')

        with self.tracer.start_as_current_span('root'):
            SqsQueueClient().add_message('default', 'msg', 0)

        messages = queue.receive_messages(MessageAttributeNames=['All'])
        self.assertEqual(len(messages), 1)
        attrs = messages[0].message_attributes
        self.assertIsNotNone(attrs)
        self.assertEqual(attrs['traceparent']['DataType'], 'String')
        self.assertTrue(attrs['traceparent']['StringValue'].startswith('00-'))

    @mock_sqs()
    def test_roundtrip_same_trace_producer_to_consumer(self):
        queue = boto3.resource('sqs', region_name=settings.AWS_REGION).create_queue(QueueName='eb-sqs-default')

        with self.tracer.start_as_current_span('root') as root:
            SqsQueueClient().add_message('default', 'msg', 0)

        service = WorkerService()
        worker = MagicMock()
        messages = service.poll_messages(queue)
        self.assertEqual(len(messages), 1)
        service._process_message(messages[0], worker)

        worker.execute.assert_called_once_with('msg')

        producer = self._spans('eb_sqs send')[0]
        consumer = self._spans('eb_sqs process')[0]
        self.assertEqual(producer.context.trace_id, root.get_span_context().trace_id)
        self.assertEqual(consumer.context.trace_id, producer.context.trace_id)
        self.assertEqual(consumer.parent.span_id, producer.context.span_id)

    @mock_sqs()
    def test_control_without_message_attribute_names_trace_is_lost(self):
        """Positive control: receiving WITHOUT MessageAttributeNames silently breaks the link."""
        queue = boto3.resource('sqs', region_name=settings.AWS_REGION).create_queue(QueueName='eb-sqs-default')

        with self.tracer.start_as_current_span('root'):
            SqsQueueClient().add_message('default', 'msg', 0)

        messages = queue.receive_messages(AttributeNames=['ApproximateReceiveCount'])  # old behaviour
        with tracing.consumer_span(messages[0]):
            pass

        producer = self._spans('eb_sqs send')[0]
        consumer = self._spans('eb_sqs process')[0]
        self.assertNotEqual(consumer.context.trace_id, producer.context.trace_id)
        self.assertIsNone(consumer.parent)


class NoOtelTest(TestCase):
    def setUp(self):
        settings.QUEUE_PREFIX = 'eb-sqs-'

    @mock_sqs()
    def test_send_and_receive_unchanged_without_otel(self):
        queue = boto3.resource('sqs', region_name=settings.AWS_REGION).create_queue(QueueName='eb-sqs-default')

        with patch.object(tracing, 'OTEL_AVAILABLE', False):
            client = SqsQueueClient()
            with patch.object(client._get_queue('default'), 'send_message') as send_message:
                client.add_message('default', 'msg', 0)

            # Identical call to the pre-tracing code: no MessageAttributes kwarg at all.
            send_message.assert_called_once_with(MessageBody='msg', DelaySeconds=0)

            client.add_message('default', 'msg', 0)
            service = WorkerService()
            worker = MagicMock()
            messages = service.poll_messages(queue)
            service._process_message(messages[0], worker)
            worker.execute.assert_called_once_with('msg')
