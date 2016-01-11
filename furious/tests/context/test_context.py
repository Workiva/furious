#
# Copyright 2012 WebFilings, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest

from google.appengine.ext import testbed

from mock import call
from mock import Mock
from mock import patch


class TestNew(unittest.TestCase):
    """Test that new returns a new context and adds it to the registry."""

    def test_new(self):
        """Ensure new returns a new context."""
        from furious.context import Context
        from furious.context import new

        self.assertIsInstance(new(), Context)

    def test_new_auto_context(self):
        """Ensure new returns a new AutoContext when batch size is specified.
        """
        from furious.context import AutoContext
        from furious.context import new

        batch_size = 100

        context = new(batch_size=batch_size)

        self.assertIsInstance(context, AutoContext)
        self.assertEqual(context.batch_size, batch_size)

    def test_new_adds_to_registry(self):
        """Ensure new adds new contexts to the context registry."""
        from furious.context import Context
        from furious.context._local import get_local_context
        from furious.context import new

        ctx = new()

        self.assertIsInstance(ctx, Context)
        self.assertIn(ctx, get_local_context().registry)


class TestContext(unittest.TestCase):
    """Test that the Context object functions in some basic way."""
    def setUp(self):
        import os
        import uuid

        harness = testbed.Testbed()
        harness.activate()
        harness.init_taskqueue_stub()

        # Ensure each test looks like it is in a new request.
        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    def test_context_works(self):
        """Ensure using a Context as a context manager works."""
        from furious.context import Context

        with Context():
            pass

    def test_context_requires_insert_tasks(self):
        """Ensure Contexts require a callable insert_tasks function."""
        from furious.context import Context

        self.assertRaises(TypeError, Context, insert_tasks='nope')

    def test_context_gets_id(self):
        """Ensure a new Context gets an id generated."""
        from furious.context import Context

        self.assertTrue(Context().id)

    @patch('uuid.uuid4', autospec=True)
    def test_id_added_to_options(self, uuid_patch):
        """Ensure random context id gets added to options."""
        from furious.context import Context

        id = 'random-id'
        uuid_patch.return_value.hex = id

        context = Context()

        self.assertEqual(context.id, id)
        self.assertEqual(context._options['id'], id)

    def test_context_gets_one_id(self):
        """Ensure a new Context gets an id only generated once."""
        from furious.context import Context

        context = Context()

        id1 = context.id
        id2 = context.id
        self.assertEqual(id1, id2)
        self.assertEqual(context.id, id1)

    def test_context_gets_assigned_id(self):
        """Ensure a new Context keeps its assigned id."""
        from furious.context import Context

        self.assertEqual('test_id_weee', Context(id='test_id_weee').id)

    def test_insert_success(self):
        """Ensure a new Context has an insert_success of 0."""
        from furious.context import Context

        self.assertEqual(0, Context().insert_success)

    def test_insert_failed(self):
        """Ensure a new Context has an insert_failed of 0."""
        from furious.context import Context

        self.assertEqual(0, Context().insert_failed)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_add_job_to_context_works(self, queue_add_mock):
        """Ensure adding a job works."""
        from furious.async import Async
        from furious.context import Context

        with Context() as ctx:
            job = ctx.add('test', args=[1, 2])

        self.assertIsInstance(job, Async)
        self.assertEqual(1, ctx.insert_success)
        self.assertEqual(1, queue_add_mock.call_count)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_bubbling_exceptions(self, queue_add_mock):
        """Ensure exceptions cause tasks to not insert."""
        from furious.context import Context

        class TestError(Exception):
            """Testing generated error."""

        def wrapper():
            with Context() as ctx:
                ctx.add('test', args=[1, 2])
                raise TestError('ka pow')

        self.assertRaises(TestError, wrapper)
        self.assertEqual(0, queue_add_mock.call_count)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_nested_context_works(self, queue_add_mock):
        """Ensure adding a job works."""
        from furious.async import Async
        from furious.context import Context

        with Context() as ctx:
            job = ctx.add('test', args=[1, 2])
            with Context() as ctx2:
                job2 = ctx2.add('test', args=[1, 2])

        self.assertIsInstance(job, Async)
        self.assertIsInstance(job2, Async)
        self.assertEqual(1, ctx.insert_success)
        self.assertEqual(1, ctx2.insert_success)
        self.assertEqual(2, queue_add_mock.call_count)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_add_multiple_jobs_to_context_works(self, queue_add_mock):
        """Ensure adding multiple jobs works."""
        from furious.context import Context

        with Context() as ctx:
            for _ in range(10):
                ctx.add('test', args=[1, 2])

        self.assertEqual(1, queue_add_mock.call_count)
        self.assertEqual(10, len(queue_add_mock.call_args[0][0]))
        self.assertEqual(10, ctx.insert_success)

    @patch('google.appengine.api.taskqueue.Queue', auto_spec=True)
    def test_added_asyncs_get_context_id(self, queue_mock):
        """Ensure Asyncs added to context get context id."""
        from furious.async import Async
        from furious.context import Context

        asyncs = [Async('test', id=i) for i in xrange(100, 110)]

        with Context() as ctx:
            for async in asyncs:
                ctx.add(async)
                self.assertEqual(ctx.id, async.get_options()['_context_id'])

        self.assertEqual(10, ctx.insert_success)

    @patch('google.appengine.api.taskqueue.Queue', auto_spec=True)
    def test_added_to_correct_queue(self, queue_mock):
        """Ensure jobs are added to the correct queue."""
        from furious.context import Context

        with Context() as ctx:
            ctx.add('test', args=[1, 2], queue='A')
            ctx.add('test', args=[1, 2], queue='A')

        queue_mock.assert_called_once_with(name='A')
        self.assertEqual(2, ctx.insert_success)

    def test_add_jobs_to_multiple_queues(self):
        """Ensure adding jobs to multiple queues works as expected."""
        from google.appengine.api.taskqueue import Queue
        from furious.context import Context

        queue_registry = {}

        class AwesomeQueue(Queue):
            def __init__(self, *args, **kwargs):
                super(AwesomeQueue, self).__init__(*args, **kwargs)

                queue_registry[kwargs.get('name')] = self
                self._calls = []

            def add(self, *args, **kwargs):
                self._calls.append((args, kwargs))

        with patch('google.appengine.api.taskqueue.Queue', AwesomeQueue):
            with Context() as ctx:
                ctx.add('test', args=[1, 2], queue='A')
                ctx.add('test', args=[1, 2], queue='A')
                ctx.add('test', args=[1, 2], queue='B')
                ctx.add('test', args=[1, 2], queue='C')

        self.assertEqual(2, len(queue_registry['A']._calls[0][0][0]))
        self.assertEqual(1, len(queue_registry['B']._calls[0][0][0]))
        self.assertEqual(1, len(queue_registry['C']._calls[0][0][0]))
        self.assertEqual(4, ctx.insert_success)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_add_task_fails(self, queue_add_mock):
        """Ensure insert_failed and insert_success are calculated correctly."""
        from google.appengine.api.taskqueue import TaskAlreadyExistsError
        from furious.context import Context

        def queue_add(tasks, transactional=False):
            if len(tasks) != 2:
                raise TaskAlreadyExistsError()

        queue_add_mock.side_effect = queue_add

        with Context() as ctx:
            ctx.add('test', args=[1, 2], queue='A')
            ctx.add('test', args=[1, 2], queue='B')
            ctx.add('test', args=[1, 2], queue='B')

        self.assertEqual(2, ctx.insert_success)
        self.assertEqual(1, ctx.insert_failed)

    def test_to_dict(self):
        """Ensure to_dict returns a dictionary representation of the Context.
        """
        import copy

        from furious.context import Context

        options = {
            'persistence_engine': 'persistence_engine',
            'unkown': True,
            'id': 'anid'
        }

        context = Context(**copy.deepcopy(options))

        # This stuff gets dumped out by to_dict().
        options.update({
            'insert_tasks': 'furious.context.context._insert_tasks',
            '_tasks_inserted': False,
            '_task_ids': [],
            'id': 'anid'
        })

        self.assertEqual(options, context.to_dict())

    def test_to_dict_with_callbacks(self):
        """Ensure to_dict correctly encodes callbacks."""
        import copy

        from furious.async import Async
        from furious.context import Context

        options = {
            'id': 'someid',
            'context_id': 'contextid',
            'parent_id': 'parentid',
            'persistence_engine': 'persistence_engine',
            'callbacks': {
                'success': self.__class__.test_to_dict_with_callbacks,
                'failure': "failure_function",
                'exec': Async(target=dir, id='blargh', context_id='contextid',
                              parent_id='parentid')
            }
        }

        context = Context(**copy.deepcopy(options))

        # This stuff gets dumped out by to_dict().
        options.update({
            'id': 'someid',
            'insert_tasks': 'furious.context.context._insert_tasks',
            'persistence_engine': 'persistence_engine',
            '_tasks_inserted': False,
            '_task_ids': [],
            'callbacks': {
                'success': ("furious.tests.context.test_context."
                            "TestContext.test_to_dict_with_callbacks"),
                'failure': "failure_function",
                'exec': {'job': ('dir', None, None),
                         'id': 'blargh',
                         'context_id': 'contextid',
                         'parent_id': 'parentid',
                         '_recursion': {'current': 0, 'max': 100},
                         '_type': 'furious.async.Async'}
            }
        })

        self.assertEqual(options, context.to_dict())

    def test_from_dict(self):
        """Ensure from_dict returns the correct Context object."""
        from furious.context import Context

        from furious.context.context import _insert_tasks

        # TODO: persistence_engine needs set to a real persistence module.

        options = {
            'id': 123456,
            'insert_tasks': 'furious.context.context._insert_tasks',
            'random_option': 'avalue',
            '_tasks_inserted': True,
            '_task_ids': [1, 2, 3, 4],
            'persistence_engine': 'furious.context.context.Context'
        }

        context = Context.from_dict(options)

        self.assertEqual(123456, context.id)
        self.assertEqual([1, 2, 3, 4], context.task_ids)
        self.assertEqual(True, context._tasks_inserted)
        self.assertEqual('avalue', context._options.get('random_option'))
        self.assertEqual(_insert_tasks, context._insert_tasks)
        self.assertEqual(Context, context._persistence_engine)

    def test_from_dict_with_callbacks(self):
        """Ensure from_dict reconstructs the Context callbacks correctly."""
        from furious.context import Context

        callbacks = {
            'success': ("furious.tests.context.test_context."
                        "TestContext.test_to_dict_with_callbacks"),
            'failure': "dir",
            'exec': {'job': ('id', None, None), 'id': 'myid',
                     'context_id': 'contextid',
                     'parent_id': 'parentid'}
        }

        context = Context.from_dict({'callbacks': callbacks})

        check_callbacks = {
            'success': TestContext.test_to_dict_with_callbacks,
            'failure': dir
        }

        callbacks = context._options.get('callbacks')
        exec_callback = callbacks.pop('exec')

        correct_dict = {'job': ('id', None, None),
                        'parent_id': 'parentid',
                        'id': 'myid',
                        'context_id': 'contextid',
                        '_recursion': {'current': 0, 'max': 100},
                        '_type': 'furious.async.Async'}

        self.assertEqual(check_callbacks, callbacks)
        self.assertEqual(correct_dict, exec_callback.to_dict())

    def test_reconstitution(self):
        """Ensure to_dict(job.from_dict()) returns the same thing."""
        from furious.context import Context

        options = {
            'id': 123098,
            'insert_tasks': 'furious.context.context._insert_tasks',
            'context_id': 'contextid',
            'persistence_engine':
            'furious.job_utils.get_function_path_and_options',
            '_tasks_inserted': True,
            '_task_ids': []
        }

        context = Context.from_dict(options)

        self.assertEqual(options, context.to_dict())

    def test_persist_with_no_engine(self):
        """Calling persist with no engine should blow up."""
        from furious.context import Context

        context = Context()
        self.assertRaises(RuntimeError, context.persist)

    def test_persist_persists(self):
        """Calling persist with an engine persists the Context."""
        from furious.context import Context

        persistence_engine = Mock()
        persistence_engine.func_name = 'persistence_engine'
        persistence_engine.im_class.__name__ = 'engine'

        context = Context(persistence_engine=persistence_engine)

        context.persist()

        persistence_engine.store_context.assert_called_once_with(context)

    def test_load_context(self):
        """Calling load with an engine attempts to load the Context."""
        from furious.context import Context

        persistence_engine = Mock()
        persistence_engine.func_name = 'persistence_engine'
        persistence_engine.im_class.__name__ = 'engine'
        persistence_engine.load_context.return_value = Context.from_dict(
            {'id': 'ABC123'})

        context = Context.load('ABC123', persistence_engine)

        persistence_engine.load_context.assert_called_once_with('ABC123')
        self.assertEqual('ABC123', context.id)


class TestInsertTasks(unittest.TestCase):
    """Test that _insert_tasks behaves as expected."""
    def setUp(self):
        harness = testbed.Testbed()
        harness.activate()
        harness.init_taskqueue_stub()

    def test_no_tasks_doesnt_blow_up(self):
        """Ensure calling with an empty list doesn't blow up."""
        from furious.context.context import _insert_tasks

        inserted = _insert_tasks((), 'A')

        self.assertEqual(0, inserted)

    @patch('google.appengine.api.taskqueue.Queue', auto_spec=True)
    def test_queue_name_is_honored(self, queue_mock):
        """Ensure the Queue is instantiated with the name."""
        from furious.context.context import _insert_tasks

        inserted = _insert_tasks((None,), 'AbCd')
        queue_mock.assert_called_once_with(name='AbCd')
        self.assertEqual(1, inserted)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_tasks_are_passed_along(self, queue_add_mock):
        """Ensure the list of tasks are passed along."""
        from furious.context.context import _insert_tasks

        inserted = _insert_tasks(('A', 1, 'B', 'joe'), 'AbCd')
        queue_add_mock.assert_called_once_with(('A', 1, 'B', 'joe'),
                                               transactional=False)
        self.assertEqual(4, inserted)

    @patch('time.sleep')
    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_task_add_error_TransientError(self, queue_add_mock, mock_sleep):
        """Ensure a TransientError gets raised from add if we've specified not
        to retry those errors."""
        from furious.context.context import _insert_tasks
        from google.appengine.api import taskqueue
        queue_add_mock.side_effect = taskqueue.TransientError

        self.assertRaises(
            taskqueue.TransientError,
            _insert_tasks, ('A',), 'AbCd', retry_transient_errors=False
        )

        queue_add_mock.assert_called_once_with(('A',), transactional=False)

    @patch('time.sleep')
    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_task_add_error_TransientError_with_delay(self, queue_add_mock,
                                                      mock_sleep):
        """Ensure a TransientError gets retried with the retry_delay that we
        have specified."""
        from furious.context.context import _insert_tasks
        from google.appengine.api import taskqueue

        queue_add_mock.side_effect = taskqueue.TransientError

        tasks = [taskqueue.Task('A')]
        self.assertRaises(
            taskqueue.TransientError,
            _insert_tasks, tasks, 'AbCd', retry_transient_errors=True,
            retry_delay=12
        )

        self.assertEqual(queue_add_mock.call_count, 2)
        mock_sleep.assert_called_once_with(12)

    @patch('time.sleep')
    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_task_add_error_TransientError_transactional(self, queue_add_mock,
                                                         mock_sleep):
        """Ensure a TransientError gets re-raised when transactional=True"""
        from furious.context.context import _insert_tasks
        from google.appengine.api import taskqueue

        queue_add_mock.side_effect = taskqueue.TransientError

        tasks = [taskqueue.Task('A')]
        self.assertRaises(
            taskqueue.TransientError,
            _insert_tasks, tasks, 'AbCd',
            transactional=True,
            retry_transient_errors=True,
            retry_delay=1,
        )

        self.assertEqual(queue_add_mock.call_count, 1)
        self.assertEqual(mock_sleep.call_count, 0)

    @patch('time.sleep')
    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_batches_get_split_TransientError(self, queue_add_mock, mock_sleep):
        """Ensure TransientErrors retries once, and correctly returns the number
        of inserted tasks."""

        from furious.context.context import _insert_tasks
        from google.appengine.api import taskqueue

        queue_add_mock.side_effect = (taskqueue.TransientError, None)
        tasks = (taskqueue.Task('A'), taskqueue.Task('1'), taskqueue.Task('B'))

        inserted = _insert_tasks(tasks, 'AbCd')
        self.assertEqual(2, queue_add_mock.call_count)
        self.assertEqual(3, inserted)
        self.assertEqual(mock_sleep.call_count, 1)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_task_add_error_BadTaskStateError(self, queue_add_mock):
        """Ensure a BadTaskStateError doesn't get raised from add."""
        from furious.context.context import _insert_tasks

        def raise_error(*args, **kwargs):
            from google.appengine.api import taskqueue
            raise taskqueue.BadTaskStateError()

        queue_add_mock.side_effect = raise_error

        inserted = _insert_tasks(('A',), 'AbCd')
        queue_add_mock.assert_called_once_with(('A',), transactional=False)
        self.assertEqual(0, inserted)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_batches_get_split_BadTaskStateError(self, queue_add_mock):
        """Ensure a batches get split and retried on BadTaskStateErrors."""
        from furious.context.context import _insert_tasks
        from google.appengine.api import taskqueue

        def raise_error(*args, **kwargs):
            raise taskqueue.BadTaskStateError()

        queue_add_mock.side_effect = raise_error
        tasks = (taskqueue.Task('A'), taskqueue.Task('1'), taskqueue.Task('B'))

        inserted = _insert_tasks(tasks, 'AbCd')
        self.assertEqual(5, queue_add_mock.call_count)
        self.assertEqual(0, inserted)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_task_add_error_TaskAlreadyExistsError(self, queue_add_mock):
        """Ensure a TaskAlreadyExistsError doesn't get raised from add."""
        from furious.context.context import _insert_tasks

        def raise_error(*args, **kwargs):
            from google.appengine.api import taskqueue
            raise taskqueue.TaskAlreadyExistsError()

        queue_add_mock.side_effect = raise_error

        inserted = _insert_tasks(('A',), 'AbCd')
        queue_add_mock.assert_called_once_with(('A',), transactional=False)
        self.assertEqual(0, inserted)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_batches_get_split_TaskAlreadyExistsError(self, queue_add_mock):
        """Ensure a batches get split and retried on TaskAlreadyExistsErrors.
        """
        from furious.context.context import _insert_tasks
        from google.appengine.api import taskqueue

        def raise_error(*args, **kwargs):
            raise taskqueue.TaskAlreadyExistsError()

        queue_add_mock.side_effect = raise_error
        tasks = (taskqueue.Task('A'), taskqueue.Task('1'), taskqueue.Task('B'))

        inserted = _insert_tasks(tasks, 'AbCd')
        self.assertEqual(5, queue_add_mock.call_count)
        self.assertEqual(0, inserted)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_task_add_error_TombstonedTaskError(self, queue_add_mock):
        """Ensure a TombstonedTaskError doesn't get raised from add."""
        from furious.context.context import _insert_tasks

        def raise_error(*args, **kwargs):
            from google.appengine.api import taskqueue
            raise taskqueue.TombstonedTaskError()

        queue_add_mock.side_effect = raise_error

        inserted = _insert_tasks(('A',), 'AbCd')
        queue_add_mock.assert_called_once_with(('A',), transactional=False)
        self.assertEqual(0, inserted)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_batches_get_split_TombstonedTaskError(self, queue_add_mock):
        """Ensure a batches get split and retried on TombstonedTaskErrors."""
        from furious.context.context import _insert_tasks
        from google.appengine.api import taskqueue

        def raise_error(*args, **kwargs):
            raise taskqueue.TombstonedTaskError()

        queue_add_mock.side_effect = raise_error
        tasks = (taskqueue.Task('A'), taskqueue.Task('1'), taskqueue.Task('B'))

        inserted = _insert_tasks(tasks, 'AbCd')
        self.assertEqual(5, queue_add_mock.call_count)
        self.assertEqual(0, inserted)

    @patch('time.sleep')
    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_single_task_gets_retried(self, queue_add_mock, mock_sleep):
        """Ensure a single task failing causes a retry.
        """

        from furious.context.context import _insert_tasks
        from google.appengine.api import taskqueue

        queue_add_mock.side_effect = (taskqueue.TransientError, None)

        inserted = _insert_tasks((taskqueue.Task('A'),), 'AbCd')
        self.assertEqual(queue_add_mock.call_count, 2)
        self.assertEqual(inserted, 1)

    @patch('time.sleep')
    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_tasks_not_enqueued_get_retried(self, queue_add_mock, mock_sleep):
        """Ensure if a taskqueue add causes a TransientError, only the tasks
        which were not enqueued are retried.
        """

        from furious.context.context import _insert_tasks
        from google.appengine.api import taskqueue

        queue_add_mock.side_effect = (taskqueue.TransientError, None)
        tasks = (Mock(), taskqueue.Task('1'), taskqueue.Task('B'))
        tasks[0].was_enqueued = True

        inserted = _insert_tasks(tasks, 'AbCd')

        calls = [
            call(tasks, transactional=False),
            call([tasks[1], tasks[2]], transactional=False)
        ]
        self.assertEqual(queue_add_mock.call_args_list, calls)
        self.assertEqual(inserted, 3)

    @patch('time.sleep')
    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_single_task_reraises_after_failure(self, queue_add_mock,
                                                mock_sleep):
        """Ensure a repeated failure on add re-raises the exception after
        retrying.
        """

        from furious.context.context import _insert_tasks
        from google.appengine.api import taskqueue

        queue_add_mock.side_effect = taskqueue.TransientError

        self.assertRaises(taskqueue.TransientError,
                          _insert_tasks, (taskqueue.Task('A'),), 'AbDc')
        self.assertEqual(queue_add_mock.call_count, 2)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_batches_get_split_dont_reinsert_enqueued(self, queue_add_mock):
        """Ensure a batch gets split and retried on error but only tasks which
        were not enqueued are retried.
        """
        from furious.context.context import _insert_tasks
        from google.appengine.api import taskqueue

        def raise_error(*args, **kwargs):
            raise taskqueue.TombstonedTaskError()

        queue_add_mock.side_effect = raise_error
        tasks = (Mock(), taskqueue.Task('1'), taskqueue.Task('B'))
        tasks[0].was_enqueued = True

        inserted = _insert_tasks(tasks, 'AbCd')
        self.assertEqual(3, queue_add_mock.call_count)
        self.assertEqual(1, inserted)


class TestTaskBatcher(unittest.TestCase):

    def test_no_tasks(self):
        """Ensure that when to tasks are passed in, no tasks are returned."""
        from furious.context.context import _task_batcher

        self.assertEqual([], list(_task_batcher([])))

    def test_one_task(self):
        """Ensure that when one task is passed in, only one batch is returned
        with one task in it.
        """
        from furious.context.context import _task_batcher

        tasks = [1]

        result = list(_task_batcher(tasks))

        self.assertEqual(1, len(result))
        self.assertEqual(1, len(result[0]))

    def test_less_than_100_tasks(self):
        """Ensure that when less than 100 tasks are passed in, only one batch
        is returned with all the tasks in it.
        """
        from furious.context.context import _task_batcher

        tasks = 'a' * 99

        result = list(_task_batcher(tasks))

        self.assertEqual(1, len(result))
        self.assertEqual(len(tasks), len(result[0]))

    def test_more_than_100_tasks(self):
        """Ensure that when more than 100 tasks are passed in, that the
        correct number of batches are returned with the tasks in them.
        """
        from furious.context.context import _task_batcher

        tasks = 'a' * 101

        result = list(_task_batcher(tasks))

        self.assertEqual(2, len(result))
        self.assertEqual(100, len(result[0]))
        self.assertEqual(1, len(result[1]))

    def test_tasks_with_small_batch_size(self):
        """Ensure that when a batch_size parameter is smaller than 100,
        that the correct number of batches are created with the tasks in them.
        """
        from furious.context.context import _task_batcher

        tasks = 'a' * 101
        batch_size = 30

        result = list(_task_batcher(tasks, batch_size=batch_size))

        self.assertEqual(4, len(result))
        self.assertEqual(30, len(result[0]))
        self.assertEqual(30, len(result[1]))
        self.assertEqual(30, len(result[2]))
        self.assertEqual(11, len(result[3]))

    def test_more_than_100_tasks_with_large_batch_size(self):
        """Ensure that when more than 100 tasks are passed in, and the
        batch_size parameter is larger than 100, that batches with a max size
        of 100 are returned with the tasks in them.
        """
        from furious.context.context import _task_batcher

        tasks = 'a' * 101
        batch_size = 2000

        result = list(_task_batcher(tasks, batch_size=batch_size))

        self.assertEqual(2, len(result))
        self.assertEqual(100, len(result[0]))
        self.assertEqual(1, len(result[1]))

