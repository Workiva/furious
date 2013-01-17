import json
import unittest

from mock import Mock, patch


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

        self.assertEqual('default_pull', message.get_queue())

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


class MessageProcessorTestCase(unittest.TestCase):

    @patch('furious.batcher.time')
    @patch('furious.batcher.memcache')
    def test_to_task_with_no_name_passed_in(self, memcache, time):
        """Ensure that if no name is passed into the MessageProcessor that it
        creates a default unique name when creating the task.
        """
        from furious.batcher import MessageProcessor

        memcache.get.return_value = 'current-batch'
        time.time.return_value = 100

        processor = MessageProcessor('something', queue='test_queue')

        task = processor.to_task()

        self.assertEqual(task.name, 'processor-processor-current-batch-3')

    @patch('furious.batcher.time')
    @patch('furious.batcher.memcache')
    def test_to_task_with_frequency_passed_in(self, memcache, time):
        """Ensure that if a frequency is passed into the MessageProcessor that
        it uses that frequency when creating the task.
        """
        from furious.batcher import MessageProcessor

        memcache.get.return_value = 'current-batch'
        time.time.return_value = 100

        processor = MessageProcessor('something', queue='test_queue', freq=100)

        task = processor.to_task()

        self.assertEqual(task.name, 'processor-processor-current-batch-1')

    @patch('furious.batcher.time')
    @patch('furious.batcher.memcache')
    def test_to_task_with_name_passed_in(self, memcache, time):
        """Ensure that if a name is passed into the MessageProcessor that it
        uses that name when creating the task.
        """
        from furious.batcher import MessageProcessor

        memcache.get.return_value = 'current-batch'
        time.time.return_value = 100

        processor = MessageProcessor('something', queue='test_queue',
                                     task_args={'name': 'test-name'})

        task = processor.to_task()

        self.assertEqual(task.name, 'test-name-processor-current-batch-3')

    @patch('furious.batcher.time')
    @patch('furious.batcher.memcache')
    def test_to_task_with_tag_passed_in(self, memcache, time):
        """Ensure that if a tag is passed into the MessageProcessor that it
        uses that tag when creating the task.
        """
        from furious.batcher import MessageProcessor

        memcache.get.return_value = 'current-batch'
        time.time.return_value = 100

        processor = MessageProcessor('something', queue='test_queue',
                                     tag='test-tag')

        task = processor.to_task()

        self.assertEqual(task.name, 'processor-test-tag-current-batch-3')

        memcache.get.assert_called_once_with('agg-batch-test-tag')

    @patch('furious.batcher.time')
    @patch('furious.batcher.memcache')
    def test_to_task_with_tag_not_passed_in(self, memcache, time):
        """Ensure that if a tag is not passed into the MessageProcessor that it
        uses a default value when creating the task.
        """
        from furious.batcher import MessageProcessor

        memcache.get.return_value = 'current-batch'
        time.time.return_value = 100

        processor = MessageProcessor('something', queue='test_queue')

        task = processor.to_task()

        self.assertEqual(task.name, 'processor-processor-current-batch-3')

        memcache.get.assert_called_once_with('agg-batch-processor')

    @patch('google.appengine.api.taskqueue.Task', autospec=True)
    @patch('furious.batcher.time')
    @patch('furious.batcher.memcache')
    def test_to_task_has_correct_arguments(self, memcache, time, task):
        """Ensure that if no name is passed into the MessageProcessor that it
        creates a default unique name when creating the task.
        """
        from furious.batcher import MessageProcessor

        memcache.get.return_value = 'current-batch'
        time.time.return_value = 100

        processor = MessageProcessor('something', queue='test_queue')

        processor.to_task()

        task_args = {
            'url': '/_ah/queue/async/something',
            'headers': {},
            'payload': json.dumps({
                'queue': 'test_queue',
                'job': ["something", None, None],
                'task_args': {
                    'countdown': 30,
                    'name': 'processor-processor-current-batch-3'
                },
            }),
            'countdown': 30,
            'name': 'processor-processor-current-batch-3'
        }

        task.assert_called_once_with(**task_args)

    @patch('furious.batcher.memcache')
    def test_curent_batch_key_exists_in_cache(self, cache):
        """Ensure that if the current batch key exists in cache that it uses it
        and doesn't update it.
        """
        from furious.batcher import MessageProcessor

        cache.get.return_value = 1

        processor = MessageProcessor('something')

        current_batch = processor.current_batch

        cache.get.assert_called_once_with('agg-batch-processor')

        self.assertEqual(current_batch, 1)
        self.assertFalse(cache.add.called)

    @patch('furious.batcher.memcache')
    def test_curent_batch_key_doesnt_exist_in_cache(self, cache):
        """Ensure that if the current batch key doesn't exist in cache that it
        inserts the default value of 1 into cache and returns it.
        """
        from furious.batcher import MessageProcessor

        cache.get.return_value = None

        processor = MessageProcessor('something')

        current_batch = processor.current_batch

        self.assertEqual(current_batch, 1)

        cache.get.assert_called_once_with('agg-batch-processor')
        cache.add.assert_called_once_with('agg-batch-processor', 1)


class InsertMessageProcessorTestCase(unittest.TestCase):

    def test_no_countdown_or_name(self):
        """Ensure that if no countdown or name are set that they aren't passed
        in as task_args to the MessageProcessor.
        """
        from furious.batcher import insert_messsage_processor
        from furious.batcher import MessageProcessor

        with patch.object(MessageProcessor, 'start') as start:
            processor = insert_messsage_processor(
                'job', None, None, 'queue-name')

            options = {
                'queue': 'queue-name',
                'job': ('job', None, None),
                'task_args': {}
            }
            self.assertEqual(processor.get_options(), options)
            self.assertEqual(processor.tag, 'processor')
            self.assertEqual(processor.frequency, 30)

        start.assert_called_once_with()

    def test_countdown_passed_in(self):
        """Ensure that if countdown is set that it's passed as a task_arg to
        the MessageProcessor.
        """
        from furious.batcher import insert_messsage_processor
        from furious.batcher import MessageProcessor

        with patch.object(MessageProcessor, 'start') as start:
            processor = insert_messsage_processor(
                'job', None, None, 'queue-name', countdown=100)

            task_args = {
                'countdown': 100
            }
            self.assertEqual(processor.get_task_args(), task_args)

        start.assert_called_once_with()

    def test_name_passed_in(self):
        """Ensure that if name is set that it's passed as a task_arg to the
        MessageProcessor.
        """
        from furious.batcher import insert_messsage_processor
        from furious.batcher import MessageProcessor

        with patch.object(MessageProcessor, 'start') as start:
            processor = insert_messsage_processor(
                'job', None, None, 'queue-name', name='name')

            task_args = {
                'name': 'name'
            }
            self.assertEqual(processor.get_task_args(), task_args)

        start.assert_called_once_with()

    def test_tag_passed_in(self):
        """Ensure that if tag is set that it's set on the MessageProcessor."""

        from furious.batcher import insert_messsage_processor
        from furious.batcher import MessageProcessor

        with patch.object(MessageProcessor, 'start') as start:
            processor = insert_messsage_processor(
                'job', None, None, 'queue-name', tag='tag')

            self.assertEqual(processor.tag, 'tag')

        start.assert_called_once_with()
