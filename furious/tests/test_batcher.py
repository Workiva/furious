import json
import unittest

from mock import patch


class MessageTestCase(unittest.TestCase):

    def test_options_are_set(self):
        """Ensure options passed to init are set on the message."""
        from furious.batcher import Message

        options = {'value': 1, 'other': 'zzz', 'nested': {1: 1}}

        message = Message(value=1, other='zzz', nested={1: 1})

        self.assertEqual(options, message._options)

    def test_update_options(self):
        """Ensure update_options updates the options."""
        from furious.batcher import Message

        options = {'value': 1, 'other': 'zzz', 'nested': {1: 1}}

        message = Message()
        message.update_options(**options.copy())

        self.assertEqual(options, message._options)

    def test_update_options_supersede_init_opts(self):
        """Ensure update_options supersedes the options set in init."""
        from furious.batcher import Message

        options = {'value': 1, 'other': 'zzz', 'nested': {1: 1}}

        message = Message(**options)

        message.update_options(value=23, other='stuff')

        options['value'] = 23
        options['other'] = 'stuff'

        self.assertEqual(options, message._options)

    def test_get_options(self):
        """Ensure get_options returns the message options."""
        from furious.batcher import Message

        options = {'value': 1, 'other': 'zzz', 'nested': {1: 1}}

        message = Message()
        message._options = options

        self.assertEqual(options, message.get_options())

    def test_get_queue(self):
        """Ensure get_queue returns the message queue."""
        from furious.batcher import Message

        queue = "test"

        message = Message(queue=queue)

        self.assertEqual(queue, message.get_queue())

    def test_get_default_queue(self):
        """Ensure get_queue returns the default queue if non was given."""
        from furious.batcher import Message

        message = Message()

        self.assertEqual('default', message.get_queue())

    def test_get_task_args(self):
        """Ensure get_task_args returns the message task_args."""
        from furious.batcher import Message

        task_args = {'other': 'zzz', 'nested': 1}
        options = {'task_args': task_args}

        message = Message(**options)

        self.assertEqual(task_args, message.get_task_args())

    def test_get_empty_task_args(self):
        """Ensure get_task_args returns {} if no task_args."""
        from furious.batcher import Message

        message = Message()

        self.assertEqual({}, message.get_task_args())

    def test_to_dict(self):
        """Ensure to_dict returns a dictionary representation of the
        Message.
        """
        from furious.batcher import Message

        task_args = {'other': 'zzz', 'nested': 1}
        options = {'task_args': task_args}

        message = Message(**options.copy())

        self.assertEqual(options, message.to_dict())

    def test_from_dict(self):
        """Ensure from_dict returns the correct Message object."""
        from furious.batcher import Message

        task_args = {'other': 'zzz', 'nested': 1}

        options = {'task_args': task_args}

        message = Message.from_dict(options)

        self.assertEqual(task_args, message.get_task_args())

    def test_reconstitution(self):
        """Ensure to_dict(job.from_dict()) returns the same thing."""
        from furious.batcher import Message

        task_args = {'other': 'zzz', 'nested': 1}
        options = {'task_args': task_args}

        message = Message.from_dict(options)

        self.assertEqual(options, message.to_dict())

    def test_to_task_with_payload(self):
        """Ensure to_task with a payload produces the right task object."""
        import datetime
        import time

        from google.appengine.ext import testbed

        from furious.batcher import Message

        testbed = testbed.Testbed()
        testbed.activate()

        # This just drops the microseconds.  It is a total mess, but is needed
        # to handle all the rounding crap.
        eta = datetime.datetime.now() + datetime.timedelta(minutes=43)
        eta_posix = time.mktime(eta.timetuple())

        task_args = {'eta': eta_posix, 'payload': [1, 2, 3]}
        options = {'task_args': task_args}

        task = Message.from_dict(options).to_task()

        # App Engine sets these headers by default.
        full_headers = {
            'Host': 'testbed.example.com',
            'X-AppEngine-Current-Namespace': ''
        }

        self.assertEqual(eta_posix, task.eta_posix)
        self.assertEqual(full_headers, task.headers)
        self.assertEqual(task_args['payload'], json.loads(task.payload))

    def test_to_task_without_payload(self):
        """Ensure to_task without a payload produces the right task object."""
        from google.appengine.ext import testbed

        from furious.batcher import Message

        testbed = testbed.Testbed()
        testbed.activate()

        options = {'task_args': {}}

        task = Message.from_dict(options).to_task()

        # App Engine sets these headers by default.
        full_headers = {
            'Host': 'testbed.example.com',
            'X-AppEngine-Current-Namespace': ''
        }

        self.assertEqual(full_headers, task.headers)
        self.assertIsNone(json.loads(task.payload))

    @patch('google.appengine.api.taskqueue.Queue', autospec=True)
    def test_insert(self, queue_mock):
        """Ensure the Task is inserted into the specified queue."""
        from furious.batcher import Message

        message = Message(queue='my_queue')
        message.insert()

        queue_mock.assert_called_once_with(name='my_queue')
        self.assertTrue(queue_mock.return_value.add.called)
