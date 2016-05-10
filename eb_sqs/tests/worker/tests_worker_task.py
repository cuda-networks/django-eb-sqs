from __future__ import absolute_import, unicode_literals

from unittest import TestCase

from eb_sqs.worker.worker_task import WorkerTask


class TestObject(object):
    def __init__(self):
        # type: () -> None
        super(TestObject, self).__init__()
        self.message = 'Test'


def dummy_function():
    pass


class SerializationTest(TestCase):
    def setUp(self):
        self.dummy_msg = '{"retry": 0, "args": [], "queue": "default", "max_retries": 5, "func": "eb_sqs.tests.worker.tests_worker_task.dummy_function", "kwargs": {}, "pickle": false}'

    def test_serialize_worker_task(self):
        worker_task = WorkerTask('default', dummy_function, [], {}, 5, 0, False)
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

    def test_serialize_pickle(self):
        worker_task1 = WorkerTask('default', dummy_function, [], {'object': TestObject()}, 5, 0, True)
        msg = worker_task1.serialize()

        worker_task2 = WorkerTask.deserialize(msg)
        self.assertEqual(worker_task2.args, worker_task1.args)
        self.assertEqual(worker_task2.kwargs['object'].message, worker_task1.kwargs['object'].message)
