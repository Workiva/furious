import json
import unittest

from mock import Mock, patch


class MessageTestCase(unittest.TestCase):

    def test_id_is_passed_in_with_options(self):
        """Ensure id is set with options passed to init are set on the message.
        """
        from furious.batcher import Message

        _id = "id"

        options = {'id': _id, 'value': 1, 'other': 'zzz', 'nested': {1: 1}}

        message = Message(id=_id, value=1, other='zzz', nested={1: 1})

        self.assertEqual(options, message._options)

    @patch('furious.batcher.uuid.uuid4')
    def test_options_are_set(self, get_uuid):
        """Ensure options passed to init are set on the message."""
        from furious.batcher import Message

        _id = "id"
        get_uuid.return_value.hex = _id

        options = {'id': _id, 'value': 1, 'other': 'zzz', 'nested': {1: 1}}

        message = Message(value=1, other='zzz', nested={1: 1})

        self.assertEqual(options, message._options)

    @patch('furious.batcher.uuid.uuid4')
    def test_update_options(self, get_uuid):
        """Ensure update_options updates the options."""
        from furious.batcher import Message

        _id = "id"
        get_uuid.return_value.hex = _id

        options = {'id': _id, 'value': 1, 'other': 'zzz', 'nested': {1: 1}}

        message = Message()
        message.update_options(**options.copy())

        self.assertEqual(options, message._options)

    @patch('furious.batcher.uuid.uuid4')
    def test_update_options_supersede_init_opts(self, get_uuid):
        """Ensure update_options supersedes the options set in init."""
        from furious.batcher import Message

        _id = "id"
        get_uuid.return_value.hex = _id

        options = {'id': _id, 'value': 1, 'other': 'zzz', 'nested': {1: 1}}

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

        self.assertEqual('default-pull', message.get_queue())

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

    @patch('furious.batcher.uuid.uuid4')
    def test_to_dict(self, get_uuid):
        """Ensure to_dict returns a dictionary representation of the
        Message.
        """
        from furious.batcher import Message

        _id = "id"
        get_uuid.return_value.hex = _id

        task_args = {'other': 'zzz', 'nested': 1}
        options = {'id': _id, 'task_args': task_args}

        message = Message(**options.copy())

        self.assertEqual(options, message.to_dict())

    def test_from_dict(self):
        """Ensure from_dict returns the correct Message object."""
        from furious.batcher import Message

        task_args = {'other': 'zzz', 'nested': 1}

        options = {'id': 'id', 'task_args': task_args}

        message = Message.from_dict(options)

        self.assertEqual(task_args, message.get_task_args())
        self.assertEqual(message.id, 'id')

    @patch('furious.batcher.uuid.uuid4')
    def test_reconstitution(self, get_uuid):
        """Ensure to_dict(job.from_dict()) returns the same thing."""
        from furious.batcher import Message

        _id = "id"
        get_uuid.return_value.hex = _id

        task_args = {'other': 'zzz', 'nested': 1}
        options = {'id': _id, 'task_args': task_args}

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

    def setUp(self):
        super(MessageProcessorTestCase, self).setUp()

        import os
        import uuid

        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    def tearDown(self):
        super(MessageProcessorTestCase, self).tearDown()

        import os

        del os.environ['REQUEST_ID_HASH']

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

    @patch('google.appengine.api.taskqueue.TaskRetryOptions', autospec=True)
    @patch('google.appengine.api.taskqueue.Task', autospec=True)
    @patch('furious.batcher.time')
    @patch('furious.batcher.memcache')
    def test_to_task_has_correct_arguments(self, memcache, time, task,
                                           task_retry):
        """Ensure that if no name is passed into the MessageProcessor that it
        creates a default unique name when creating the task.
        """
        from furious.async import MAX_RESTARTS
        from furious.batcher import MessageProcessor

        memcache.get.return_value = 'current-batch'
        time.time.return_value = 100

        task_retry_object = Mock()
        task_retry.return_value = task_retry_object

        processor = MessageProcessor('something', queue='test_queue',
                                     id='someid', parent_id='parentid',
                                     context_id="contextid")

        processor.to_task()

        task_args = {
            'name': 'processor-processor-current-batch-3',
            'url': '/_ah/queue/async/something',
            'countdown': 30,
            'headers': {},
            'retry_options': task_retry_object,
            'payload': json.dumps(processor.to_dict())
        }

        task.assert_called_once_with(**task_args)
        task_retry.assert_called_once_with(task_retry_limit=MAX_RESTARTS)

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


class BumpBatchTestCase(unittest.TestCase):

    @patch('furious.batcher.memcache')
    def test_cache_incremented_by_key(self, cache):
        """Ensure that the cache object is incremented by the key passed in."""
        from furious.batcher import bump_batch

        cache.incr.return_value = 2

        val = bump_batch('group')

        self.assertEqual(val, 2)

        cache.incr.assert_called_once_with('agg-batch-group')


class MessageIteratorTestCase(unittest.TestCase):

    def test_raise_stopiteration_if_no_messages(self):
        """Ensure MessageIterator raises StopIteration if no messages."""
        from furious.batcher import MessageIterator

        iterator = MessageIterator('tag', 'qn', 1)
        with patch.object(iterator, 'queue') as queue:
            queue.lease_tasks_by_tag.return_value = []

        self.assertRaises(StopIteration, iterator.next)

    def test_iterates(self):
        """Ensure MessageIterator instances iterate in loop."""
        from furious.batcher import MessageIterator

        payload = '["test"]'
        task = Mock(payload=payload, tag='tag')
        task1 = Mock(payload=payload, tag='tag')
        task2 = Mock(payload=payload, tag='tag')

        iterator = MessageIterator('tag', 'qn', 1)

        with patch.object(iterator, 'queue') as queue:
            queue.lease_tasks_by_tag.return_value = [task, task1, task2]

            results = [payload for payload in iterator]

        self.assertEqual(results, [payload, payload, payload])

    def test_calls_lease_exactly_once(self):
        """Ensure MessageIterator calls lease only once."""
        from furious.batcher import MessageIterator

        payload = '["test"]'
        task = Mock(payload=payload, tag='tag')

        message_iterator = MessageIterator('tag', 'qn', 1)

        with patch.object(message_iterator, 'queue') as queue:
            queue.lease_tasks_by_tag.return_value = [task]

            iterator = iter(message_iterator)
            iterator.next()

            self.assertRaises(StopIteration, iterator.next)
            self.assertRaises(StopIteration, iterator.next)

        queue.lease_tasks_by_tag.assert_called_once_with(
            60, 1, tag='tag', deadline=10)

    def test_rerun_after_depletion_calls_once(self):
        """Ensure MessageIterator works when used manually."""
        from furious.batcher import MessageIterator

        payload = '["test"]'
        task = Mock(payload=payload, tag='tag')

        iterator = MessageIterator('tag', 'qn', 1)

        with patch.object(iterator, 'queue') as queue:
            queue.lease_tasks_by_tag.return_value = [task]

            results = [payload for payload in iterator]
            self.assertEqual(results, [payload])

            results = [payload for payload in iterator]

        queue.lease_tasks_by_tag.assert_called_once_with(
            60, 1, tag='tag', deadline=10)

    def test_rerun_after_depletion_doesnt_delete_too_much(self):
        """Ensure MessageIterator works when used manually."""
        from furious.batcher import MessageIterator

        payload = '["test"]'
        task = Mock(payload=payload, tag='tag')

        iterator = MessageIterator('tag', 'qn', 1, auto_delete=False)

        with patch.object(iterator, 'queue') as queue:
            queue.lease_tasks_by_tag.return_value = [task]

            results = [payload for payload in iterator]
            self.assertEqual(results, [payload])

            # This new work should never be leased, but simulates new pending
            # work.
            task_1 = Mock(payload='["task_1"]', tag='tag')
            queue.lease_tasks_by_tag.return_value = [task_1]

            # Iterating again should return the "originally leased" work, not
            # new work.
            results = [payload for payload in iterator]
            self.assertEqual(results, [payload])

            # Lease should only have been called a single time.
            queue.lease_tasks_by_tag.assert_called_once_with(
                60, 1, tag='tag', deadline=10)

            # The delete call should delete only the original work.
            iterator.delete_messages()
            queue.delete_tasks.assert_called_once_with([task])

    def test_custom_deadline(self):
        """Ensure that a custom deadline gets passed to lease_tasks."""
        from furious.batcher import MessageIterator

        payload = '["test"]'
        task = Mock(payload=payload, tag='tag')

        message_iterator = MessageIterator('tag', 'qn', 1, deadline=2)

        with patch.object(message_iterator, 'queue') as queue:
            queue.lease_tasks_by_tag.return_value = [task]

            iterator = iter(message_iterator)
            iterator.next()

            self.assertRaises(StopIteration, iterator.next)

        queue.lease_tasks_by_tag.assert_called_once_with(
            60, 1, tag='tag', deadline=2)

    @patch('time.time')
    def test_time_check(self, time):
        """Ensure that a DeadlineExceededError is thrown when the lease takes
        over (deadline-0.1) secs.
        """
        from google.appengine.runtime import apiproxy_errors
        from furious.batcher import MessageIterator

        time.side_effect = [0.0, 9.9]
        message_iterator = MessageIterator('tag', 'qn', 1)

        with patch.object(message_iterator, 'queue') as queue:
            queue.lease_tasks_by_tag.return_value = []

            self.assertRaises(
                apiproxy_errors.DeadlineExceededError, iter, message_iterator)

