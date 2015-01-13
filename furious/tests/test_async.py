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

import json

import unittest

import mock


class TestDefaultsDecorator(unittest.TestCase):
    """Ensure that defaults decorator works as expected."""

    def test_decorated_name_is_preserved(self):
        """Ensure defaults decorator preserves decorated function's name."""
        from furious.async import defaults

        @defaults(test=None)
        def decorated_function():
            pass

        self.assertEqual('decorated_function', decorated_function.__name__)

    def test_decorate_with_options(self):
        """Ensure defaults decorator sets options on decorated function."""
        from furious.async import defaults

        options = {'test': 'me'}

        @defaults(**options)
        def decorated_function():
            pass

        self.assertEqual(options, decorated_function._async_options)

    def test_raises_on_job_in_options(self):
        """Ensure defaults decorator raise error if job in options."""
        from furious.async import defaults

        options = {'job': 'me'}

        self.assertRaises(AssertionError, defaults, **options)

    @unittest.skip('Not sure if this is needed.')
    def test_raises_on_callbacks_in_options(self):
        """Ensure defaults decorator raise error if callbacks is in options."""
        from furious.async import defaults

        options = {'callbacks': 'me'}

        self.assertRaises(AssertionError, defaults, **options)

    def test_raises_on_good_with_bad_options(self):
        """Ensure defaults decorator raises error with a mix of good/bad
        options.
        """
        from furious.async import defaults

        options = {'job': 'me', 'other': 'option'}

        self.assertRaises(AssertionError, defaults, **options)

    def test_function_is_runnable(self):
        """Ensure the decorated function still runs."""
        from furious.async import defaults

        options = {'other': 'option'}

        check_value = {'ran': False}

        @defaults(**options)
        def some_method():
            check_value['ran'] = True

        some_method()

        self.assertTrue(check_value['ran'])


class TestAsync(unittest.TestCase):
    """Make sure Async produces correct Task objects."""

    def setUp(self):
        import os
        import uuid
        from furious.context import _local

        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

        local_context = _local.get_local_context()
        local_context._executing_async_context = None

    def tearDown(self):
        import os

        del os.environ['REQUEST_ID_HASH']

    def test_none_function(self):
        """Ensure passing None as function raises."""
        from furious.async import Async
        from furious.errors import BadObjectPathError

        self.assertRaises(BadObjectPathError, Async, None)

    def test_empty_function_path(self):
        """Ensure passing None as function raises."""
        from furious.async import Async
        from furious.errors import BadObjectPathError

        self.assertRaises(BadObjectPathError, Async, '')

    def test_job_params(self):
        """Ensure good args and kwargs generate a well-formed job tuple."""
        from furious.async import Async

        job = ("test", [1, 2, 3], {'a': 1, 'b': 2, 'c': 3})
        async_job = Async(*job)

        self.assertEqual(job, async_job.job)

    def test_no_args_or_kwargs(self):
        """Ensure no args and no kwargs generate a well-formed job tuple."""
        from furious.async import Async

        function = "test.func"
        async_job = Async(function)

        self.assertEqual(function, async_job._function_path)
        self.assertEqual((function, None, None), async_job.job)

    def test_args_with_no_kwargs(self):
        """Ensure args and no kwargs generate a well-formed job tuple."""
        from furious.async import Async

        job = ("test", (1, 2, 3))
        async_job = Async(*job)

        self.assertEqual(job + (None,), async_job.job)

    def test_no_args_with_kwargs(self):
        """Ensure no args with kwargs generate a well-formed job tuple."""
        from furious.async import Async

        job = ("test", None, {'a': 1, 'b': 'c', 'alpha': True})
        async_job = Async(*job)

        self.assertEqual(job, async_job.job)

    def test_gets_callable_path(self):
        """Ensure the job tuple contains the callable path."""
        from furious.async import Async

        def some_function():
            """Will look like is at the module level."""
            pass

        job_args = ([1, 2, 3], {'a': 1, 'b': 2, 'c': 3})
        async_job = Async(some_function, *job_args)

        self.assertEqual(
            ('furious.tests.test_async.some_function',) + job_args,
            async_job.job)

    def test_none_args_and_kwargs(self):
        """Ensure args and kwargs may be None."""
        from furious.async import Async

        job = ("something", None, None,)
        async_job = Async(*job)

        self.assertEqual(job, async_job.job)

    @mock.patch('uuid.uuid4', autospec=True)
    def test_generates_id(self, uuid_patch):
        """Ensure an id is auto-generated if not specified."""
        from furious.async import Async

        id = 'random-id'
        uuid_patch.return_value.hex = id

        job = Async('somehting')

        self.assertEqual(job.id, id)
        self.assertEqual(job.get_options()['id'], id)

    def test_generates_one_id(self):
        """Ensure only one random id is auto-generated if not specified."""
        from furious.async import Async

        job = Async('somehting')

        id1 = job.id
        id2 = job.id
        self.assertEqual(id1, id2)
        self.assertEqual(job.id, id1)

    def test_uses_given_id(self):
        """Ensure an id passed in is used."""
        from furious.async import Async

        job = Async('somehting', id='superrandom')

        self.assertEqual(job.id, 'superrandom')
        self.assertEqual(job.get_options()['id'], 'superrandom')

    def test_update_id(self):
        """Ensure using update options to update an id works."""
        from furious.async import Async

        job = Async('somehting')
        job.update_options(id='newid')

        self.assertEqual(job.id, 'newid')
        self.assertEqual(job.get_options()['id'], 'newid')

    def test_context_id(self):
        """Ensure context_id returns the context_id."""
        from furious.async import Async

        job = Async('somehting')
        job.update_options(context_id='blarghahahaha')
        self.assertEqual(job.context_id, 'blarghahahaha')

    def test_no_context_id(self):
        """Ensure calling context_id when none exists returns None."""
        from furious.async import Async

        job = Async('somehting')
        self.assertIsNone(job.context_id)

    def test_decorated_options(self):
        """Ensure the defaults decorator sets Async options."""
        from furious.async import Async
        from furious.async import defaults

        options = {'value': 1, 'other': 'zzz', 'nested': {1: 1}, 'id': 'thing',
                   'context_id': None, 'parent_id': 'parentid'}

        @defaults(**options.copy())
        def some_function():
            pass

        job = Async(some_function)

        options['job'] = ("furious.tests.test_async.some_function", None, None)
        options['_recursion'] = {'current': 0, 'max': 100}

        self.assertEqual(options, job._options)

    def test_init_opts_supersede_decorated_options(self):
        """Ensure options passed to init override decorated options."""
        from furious.async import Async
        from furious.async import defaults

        options = {'value': 1, 'other': 'zzz', 'nested': {1: 1}, 'id': 'wrong',
                   'context_id': None, 'parent_id': 'parentid'}

        @defaults(**options.copy())
        def some_function():
            pass

        job = Async(some_function, value=17, other='abc', id='correct')

        options['value'] = 17
        options['other'] = 'abc'
        options['id'] = 'correct'

        options['job'] = ("furious.tests.test_async.some_function", None, None)
        options['_recursion'] = {'current': 0, 'max': 100}

        self.assertEqual(options, job._options)

    def test_set_execution_context(self):
        """Ensure set_execution_context doesn't blow up."""
        from furious.async import Async
        Async(target=dir).set_execution_context(object())

    def test_set_execution_context_requires_context(self):
        """Ensure set_execution_context requires a context argument."""
        from furious.async import Async
        async = Async(target=dir)
        self.assertRaises(TypeError, async.set_execution_context)

    def test_set_execution_context_disallows_double_set(self):
        """Ensure calling set_execution_context twice raises
        AlreadyInContextError.
        """
        from furious.async import Async
        from furious.errors import AlreadyInContextError

        async = Async(target=dir)
        async.set_execution_context(object())
        self.assertRaises(
            AlreadyInContextError, async.set_execution_context, object())

    def test_update_options(self):
        """Ensure update_options updates the options."""
        from furious.async import Async

        options = {'value': 1, 'other': 'zzz', 'nested': {1: 1}, 'id': 'xx',
                   'context_id': None, 'parent_id': 'parentid'}

        job = Async("nonexistant")
        job.update_options(**options.copy())

        options['job'] = ("nonexistant", None, None)

        options['_recursion'] = {'current': 0, 'max': 100}

        self.assertEqual(options, job._options)

    def test_update_options_supersede_init_opts(self):
        """Ensure update_options supersedes the options set in init."""
        from furious.async import Async

        options = {'value': 1, 'other': 'zzz', 'nested': {1: 1}, 'id': 'wrong',
                   'context_id': None, 'parent_id': 'parentid'}

        job = Async("nonexistant", **options.copy())

        job.update_options(value=23, other='stuff', id='right')

        options['value'] = 23
        options['other'] = 'stuff'
        options['id'] = 'right'

        options['job'] = ("nonexistant", None, None)

        options['_recursion'] = {'current': 0, 'max': 100}

        self.assertEqual(options, job._options)

    def test_get_options(self):
        """Ensure get_options returns the job options."""
        from furious.async import Async

        options = {'value': 1, 'other': 'zzz', 'nested': {1: 1}}

        job = Async("nonexistant")
        job._options = options

        self.assertEqual(options, job.get_options())

    def test_get_headers(self):
        """Ensure get_headers returns the job headers."""
        from furious.async import Async

        headers = {'other': 'zzz', 'nested': 1}
        options = {'headers': headers}

        job = Async('nonexistant', **options)

        self.assertEqual(headers, job.get_headers())

    def test_get_empty_headers(self):
        """Ensure get_headers returns the job headers."""
        from furious.async import Async

        job = Async('nonexistant')

        self.assertEqual({}, job.get_headers())

    def test_get_queue(self):
        """Ensure get_queue returns the job queue."""
        from furious.async import Async

        queue = "test"

        job = Async('nonexistant', queue=queue)

        self.assertEqual(queue, job.get_queue())

    def test_get_default_queue(self):
        """Ensure get_queue returns the default queue if non was given."""
        from furious.async import Async

        job = Async('nonexistant')

        self.assertEqual('default', job.get_queue())

    def test_get_task_args(self):
        """Ensure get_task_args returns the job task_args."""
        from furious.async import Async

        task_args = {'other': 'zzz', 'nested': 1}
        options = {'task_args': task_args}

        job = Async('nonexistant', **options)

        self.assertEqual(task_args, job.get_task_args())

    def test_get_empty_task_args(self):
        """Ensure get_task_args returns {} if no task_args."""
        from furious.async import Async

        job = Async('nonexistant')

        self.assertEqual({}, job.get_task_args())

    def test_deepcopy(self):
        """Make sure you can deepcopy an Async."""
        import copy

        from furious.async import Async

        job = Async(dir)
        copy.deepcopy(job)

    def test_to_dict(self):
        """Ensure to_dict returns a dictionary representation of the Async."""
        from furious.async import Async

        task_args = {'other': 'zzz', 'nested': 1}
        headers = {'some': 'thing', 'fun': 1}
        options = {'headers': headers, 'task_args': task_args, 'id': 'me',
                   'context_id': None, 'parent_id': 'parentid'}

        job = Async('nonexistant', **options.copy())

        options['job'] = ('nonexistant', None, None)
        options['_recursion'] = {'current': 0, 'max': 100}
        options['_type'] = 'furious.async.Async'

        self.assertEqual(options, job.to_dict())

    def test_to_dict_with_callbacks(self):
        """Ensure to_dict correctly encodes callbacks."""
        from furious.async import Async

        options = {'id': 'anident',
                   'context_id': 'contextid',
                   'parent_id': 'parentid',
                   'callbacks': {
                       'success': self.__class__.test_to_dict_with_callbacks,
                       'failure': "failure_function",
                       'exec': Async(target=dir, id='subidnet',
                                     parent_id='parentid'),
                   }}

        job = Async('nonexistant', **options.copy())

        options['job'] = ('nonexistant', None, None)
        options['callbacks'] = {
            'success': ("furious.tests.test_async."
                        "TestAsync.test_to_dict_with_callbacks"),
            'failure': "failure_function",
            'exec': {'job': ('dir', None, None),
                     'id': 'subidnet',
                     'context_id': None,
                     'parent_id': 'parentid',
                     '_recursion': {'current': 0, 'max': 100},
                     '_type': 'furious.async.Async'}
        }
        options['_recursion'] = {'current': 0, 'max': 100}
        options['_type'] = 'furious.async.Async'

        self.assertEqual(options, job.to_dict())

    def test_from_dict(self):
        """Ensure from_dict returns the correct Async object."""
        from furious.async import Async

        headers = {'some': 'thing', 'fun': 1}
        job = ('test', None, None)
        task_args = {'other': 'zzz', 'nested': 1}

        options = {'job': job, 'headers': headers, 'task_args': task_args}

        async_job = Async.from_dict(options)

        self.assertEqual(headers, async_job.get_headers())
        self.assertEqual(task_args, async_job.get_task_args())
        self.assertEqual(job[0], async_job._function_path)
        self.assertEqual(job[0], async_job.function_path)

    def test_from_dict_with_callbacks(self):
        """Ensure from_dict reconstructs callbacks correctly."""
        from furious.async import Async

        job = ('test', None, None)
        callbacks = {
            'success': ("furious.tests.test_async."
                        "TestAsync.test_to_dict_with_callbacks"),
            'failure': "dir",
            'exec': {'job': ('dir', None, None), 'id': 'petey',
                     'parent_id': 'parentid'}
        }

        options = {'job': job, 'callbacks': callbacks, 'parent_id': 'parentid'}

        async_job = Async.from_dict(options)

        check_callbacks = {
            'success': TestAsync.test_to_dict_with_callbacks,
            'failure': dir
        }

        callbacks = async_job.get_callbacks()
        exec_callback = callbacks.pop('exec')

        correct_options = {'job': ('dir', None, None),
                           'id': 'petey',
                           'parent_id': 'parentid',
                           'context_id': None,
                           '_recursion': {'current': 0, 'max': 100},
                           '_type': 'furious.async.Async'}

        self.assertEqual(check_callbacks, callbacks)
        self.assertEqual(correct_options, exec_callback.to_dict())

    def test_reconstitution(self):
        """Ensure to_dict(job.from_dict()) returns the same thing."""
        from furious.async import Async

        headers = {'some': 'thing', 'fun': 1}
        job = ('test', None, None)
        task_args = {'other': 'zzz', 'nested': 1}
        options = {
            'job': job,
            'id': 'someid',
            'headers': headers,
            'task_args': task_args,
            'persistence_engine': 'furious.extras.appengine.ndb_persistence',
            '_recursion': {'current': 1, 'max': 100},
            '_type': 'furious.async.Async',
            'context_id': None,
            'parent_id': 'parentid'
        }

        async_job = Async.from_dict(options)

        self.assertEqual(options, async_job.to_dict())

    def test_to_task(self):
        """Ensure to_task produces the right task object."""
        import datetime
        import time

        from google.appengine.ext import testbed

        from furious.async import Async
        from furious.async import ASYNC_ENDPOINT

        testbed = testbed.Testbed()
        testbed.activate()

        # This just drops the microseconds.  It is a total mess, but is needed
        # to handle all the rounding crap.
        eta = datetime.datetime.now() + datetime.timedelta(minutes=43)
        eta_posix = time.mktime(eta.timetuple())

        headers = {'some': 'thing', 'fun': 1}

        job = ('test', None, None)

        expected_url = "%s/%s" % (ASYNC_ENDPOINT, 'test')

        task_args = {'eta': eta_posix}
        options = {'job': job, 'headers': headers, 'task_args': task_args,
                   'id': 'ident', 'context_id': 'contextid',
                   'parent_id': 'parentid'}

        task = Async.from_dict(options).to_task()

        # App Engine sets these headers by default.
        full_headers = {
            'Host': 'testbed.example.com',
            'X-AppEngine-Current-Namespace': ''
        }
        full_headers.update(headers)

        self.assertEqual(eta_posix, task.eta_posix)
        self.assertEqual(expected_url, task.url)
        self.assertEqual(full_headers, task.headers)

        options['task_args']['eta'] = datetime.datetime.fromtimestamp(
            eta_posix)

        options['_recursion'] = {'current': 1, 'max': 100}
        options['_type'] = 'furious.async.Async'

        self.assertEqual(
            options, Async.from_dict(json.loads(task.payload)).get_options())

    def test_getting_result_fails(self):
        """Ensure attempting to get the result before executing raises."""
        from furious.async import Async
        from furious.errors import NotExecutedError

        job = Async(target=dir)

        def get_result():
            return job.result

        self.assertRaises(NotExecutedError, get_result)
        self.assertFalse(job.executed)

    def test_getting_result(self):
        """Ensure getting the result after executing works."""
        from furious.async import Async

        job = Async(target=dir)
        job._executing = True
        job.result = 123456

        self.assertEqual(123456, job.result)
        self.assertTrue(job.executed)

    def test_setting_result_fails(self):
        """Ensure the result can not be set without the execute flag set."""
        from furious.async import Async
        from furious.errors import NotExecutingError

        job = Async(target=dir)

        def set_result():
            job.result = 123

        self.assertRaises(NotExecutingError, set_result)
        self.assertFalse(job.executed)

    def test_setting_result(self):
        """Ensure the result can be set if the execute flag is set."""
        from furious.async import Async

        job = Async(target=dir)
        job.executing = True
        job.result = 123
        self.assertEqual(123, job.result)
        self.assertTrue(job.executed)

    def test_setting_result_does_not_call_persist(self):
        """Ensure setting the result doesn't call persist result if not in
        persist mode.
        """
        from furious.async import Async

        result = "here be the results."

        persistence_engine = mock.Mock()

        job = Async(target=dir)

        # Manually set the persistence_engine so that the Async doesn't try to
        # reload the mock persistence_engine.
        job._persistence_engine = persistence_engine

        job.executing = True
        job.result = result

        self.assertEqual(persistence_engine.store_async_result.call_count, 0)

    def test_setting_result_calls_persist(self):
        """Ensure setting the result calls the persist_result method."""
        from furious.async import Async

        result = "here be the results."

        persistence_engine = mock.Mock()

        job = Async(target=dir, persist_result=True)

        # Manually set the persistence_engine so that the Async doesn't try to
        # reload the mock persistence_engine.
        job._persistence_engine = persistence_engine

        job.executing = True
        job.result = result

        persistence_engine.store_async_result.assert_called_once_with(job.id,
                                                                      result)

    @mock.patch('time.sleep')
    @mock.patch('google.appengine.api.taskqueue.Queue', autospec=True)
    def test_start_hits_transient_error(self, queue_mock, mock_sleep):
        """Ensure the task retries if a transient error is hit."""
        from google.appengine.api.taskqueue import TransientError
        from furious.async import Async

        def add(task, *args, **kwargs):
            def add_second(task, *args, **kwargs):
                assert task

            queue_mock.return_value.add.side_effect = add_second
            raise TransientError()

        queue_mock.return_value.add.side_effect = add

        async_job = Async("something", queue='my_queue')
        async_job.start()

        queue_mock.assert_called_with(name='my_queue')
        self.assertEqual(2, queue_mock.return_value.add.call_count)
        self.assertEqual(1, mock_sleep.call_count)

    @mock.patch('time.sleep')
    @mock.patch('google.appengine.api.taskqueue.Queue', autospec=True)
    def test_start_hits_transient_error_retry_disabled(self, queue_mock,
                                                       sleep_mock):
        """Ensure if transient error retries are disabled, that those errors are
        re-raised immediately without any attempt to re-insert.
        """
        from google.appengine.api.taskqueue import TransientError
        from furious.async import Async

        queue_mock.return_value.add.side_effect = TransientError()

        async_job = Async("something", queue='my_queue',
                          retry_transient_errors=False)

        self.assertRaises(TransientError, async_job.start)
        self.assertEqual(1, queue_mock.return_value.add.call_count)

        # Try again with the option enabled, this should cause a retry after a
        # delay, which we have also specified.
        queue_mock.reset_mock()
        async_job = Async("something", queue='my_queue',
                          retry_transient_errors=True,
                          retry_delay=12)

        self.assertRaises(TransientError, async_job.start)
        self.assertEqual(2, queue_mock.return_value.add.call_count)
        sleep_mock.assert_called_once_with(12)

    @mock.patch('time.sleep')
    @mock.patch('google.appengine.api.taskqueue.Queue', autospec=True)
    def test_start_hits_transient_error_transactional(self, queue_mock,
                                                      sleep_mock):
        """Ensure if caller is specifying transactional, that Transient errors
        are immediately re-raised.
        """
        from google.appengine.api.taskqueue import TransientError
        from furious.async import Async

        queue_mock.return_value.add.side_effect = TransientError()

        async_job = Async("something", queue='my_queue',
                          retry_transient_errors=True)

        self.assertRaises(TransientError, async_job.start,
                          transactional=True)
        self.assertEqual(1, queue_mock.return_value.add.call_count)
        self.assertEqual(0, sleep_mock.call_count)

    @mock.patch('google.appengine.api.taskqueue.Queue', autospec=True)
    def test_start_hits_other_error_retry_enabled(self, queue_mock):
        """Ensure if transient error retries are enabled, that other errors are
        not retried.
        """

        from furious.async import Async

        queue_mock.return_value.add.side_effect = (Exception(), None)

        async_job = Async("something", queue='my_queue',
                          retry_transient_errors=True)

        self.assertRaises(Exception, async_job.start)
        self.assertEqual(1, queue_mock.return_value.add.call_count)

    @mock.patch('google.appengine.api.taskqueue.Queue', autospec=True)
    def test_start_hits_task_already_exists_error_error(self, queue_mock):
        """Ensure the task returns if a task already exists error is hit."""
        from google.appengine.api.taskqueue import TaskAlreadyExistsError
        from furious.async import Async

        queue_mock.return_value.add.side_effect = TaskAlreadyExistsError()

        async_job = Async("something", queue='my_queue')
        async_job.start()

        queue_mock.assert_called_with(name='my_queue')
        self.assertEqual(1, queue_mock.return_value.add.call_count)

    @mock.patch('google.appengine.api.taskqueue.Queue', autospec=True)
    def test_start_hits_tombstoned_task_error_error(self, queue_mock):
        """Ensure the task returns if a tombstoned task error is hit."""
        from google.appengine.api.taskqueue import TombstonedTaskError
        from furious.async import Async

        queue_mock.return_value.add.side_effect = TombstonedTaskError()

        async_job = Async("something", queue='my_queue')
        async_job.start()

        queue_mock.assert_called_with(name='my_queue')
        self.assertEqual(1, queue_mock.return_value.add.call_count)

    @mock.patch('google.appengine.api.taskqueue.Queue', autospec=True)
    def test_start_runs_successfully(self, queue_mock):
        """Ensure the Task is inserted into the specified queue."""
        from furious.async import Async

        async_job = Async("something", queue='my_queue')
        async_job.start()

        queue_mock.assert_called_once_with(name='my_queue')
        self.assertTrue(queue_mock.return_value.add.called)

        # TODO: Check that the task is the same.
        # self.assertEqual(task, queue_mock.add.call_args)

    @mock.patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_task_transactional(self, queue_add_mock):
        """Ensure the task is added transactional when start is
        called with transactional."""
        from furious.async import Async

        async_job = Async("something")
        async_job.start(transactional=True)
        call_args = queue_add_mock.call_args
        call_kwargs = call_args[1]

        self.assertIn('transactional', call_kwargs)
        self.assertTrue(call_kwargs['transactional'])

    @mock.patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_task_non_transactional(self, queue_add_mock):
        """Ensure the task is added transactional when start is
        called with transactional."""
        from furious.async import Async

        async_job = Async("something")
        async_job.start(transactional=False)
        call_args = queue_add_mock.call_args
        call_kwargs = call_args[1]

        self.assertIn('transactional', call_kwargs)
        self.assertFalse(call_kwargs['transactional'])

    @mock.patch('google.appengine.api.taskqueue.Queue.add_async',
                auto_spec=True)
    def test_start_async_no_rpc(self, queue_add_async_mock):
        """Ensure that when the task is called with async=True, that the
        add_async method is called with the default rpc=None.
        """
        from furious.async import Async

        async_job = Async("something")
        async_job.start(async=True)

        self.assertTrue(queue_add_async_mock.called)
        self.assertEqual(None, queue_add_async_mock.call_args[1]['rpc'])

    @mock.patch('google.appengine.api.taskqueue.Queue.add_async',
                auto_spec=True)
    def test_start_async_with_rpc(self, queue_add_async_mock):
        """Ensure that when the task is called with async=True and an rpc is
        provided, that the add_async method is called with the correct rpc.
        """
        from mock import Mock
        from furious.async import Async

        rpc = Mock()
        async_job = Async("something")
        async_job.start(async=True, rpc=rpc)

        self.assertTrue(queue_add_async_mock.called)
        self.assertEqual(rpc, queue_add_async_mock.call_args[1]['rpc'])

    def test_update_recursion_level_defaults(self):
        """Ensure that defaults (1, MAX_DEPTH) are set correctly."""
        from furious.async import Async
        from furious.async import MAX_DEPTH

        async_job = Async("something")

        async_job._increment_recursion_level()

        options = async_job.get_options()['_recursion']
        self.assertEqual(1, options['current'])
        self.assertEqual(MAX_DEPTH, options['max'])

    def test_check_recursion_level_execution_context(self):
        """Ensure that when there is an existing Async that the correct values
        are pulled and incremented from there, not the defaults.
        """
        from furious.async import Async
        from furious.context import execution_context_from_async

        context_async = Async("something", _recursion={'current': 42,
                                                       'max': 77})
        new_async = Async("something_else")

        with execution_context_from_async(context_async):
            new_async._increment_recursion_level()

        self.assertEqual(43, new_async.recursion_depth)

        options = new_async.get_options()['_recursion']
        self.assertEqual(77, options['max'])

    def test_check_recursion_level_overridden_interior_max(self):
        """Ensure that when there is an existing Async that the correct values
        are pulled and incremented from there, unless the interior Async sets
        it's own custom max.
        """
        from furious.async import Async
        from furious.context import execution_context_from_async

        context_async = Async("something", _recursion={'current': 42,
                                                       'max': 77})

        new_async = Async("something_else", _recursion={'max': 89})

        with execution_context_from_async(context_async):
            new_async._increment_recursion_level()

        options = new_async.get_options()['_recursion']
        self.assertEqual(43, options['current'])
        self.assertEqual(89, options['max'])

    def test_check_recursion_depth_over_limit(self):
        """Ensure that when over the recusion limit, calling
        check_recursion_depth raises a AsyncRecursionError.
        """
        from furious.async import Async
        from furious.errors import AsyncRecursionError

        async = Async("something", _recursion={'current': 8, 'max': 7})

        self.assertRaises(AsyncRecursionError, async.check_recursion_depth)

    def test_check_recursion_disabled(self):
        """Ensure that when recursion max depth is explicitly set to -1, then
        the recursion check is disabled.

        There are no explicit asserts in this test because the
        check_recursion_depth() method would throw an exception if this
        functionality wasn't working.
        """
        from furious.async import Async

        async_job = Async("something", _recursion={'current': 101,
                                                   'max': -1})

        async_job.check_recursion_depth()

    def test_retry_default(self):
        """Ensure that when no task_retry_limit specified, that the default is
        set.
        """
        from furious.async import Async
        from furious.async import MAX_RESTARTS

        async_job = Async("something")
        task = async_job.to_task()

        self.assertEqual(MAX_RESTARTS, task.retry_options.task_retry_limit)

    def test_retry_custom(self):
        """Ensure that when a custom retry limit is set, that it's
        propagated.
        """
        from furious.async import Async

        async_job = Async("something",
                          task_args={'retry_options': {'task_retry_limit': 5}})
        task = async_job.to_task()

        self.assertEqual(5, task.retry_options.task_retry_limit)

    def test_retry_value_without_to_task(self):
        """Ensure that when you encode the options, the retry_options are not
        affected.
        """
        from furious.async import Async
        from furious.async import encode_async_options

        async_job = Async("something",
                          task_args={'retry_options': {'task_retry_limit': 5}})
        options = encode_async_options(async_job)

        self.assertEqual(
            5, options['task_args']['retry_options']['task_retry_limit'])

    def test_retry_value_with_to_task(self):
        """Ensure that calling to_task doesn't affect the options when
        encoding.
        """
        from furious.async import Async
        from furious.async import encode_async_options

        async_job = Async("something",
                          task_args={'retry_options': {'task_retry_limit': 5}})
        async_job.to_task()
        options = encode_async_options(async_job)

        self.assertEqual(
            5, options['task_args']['retry_options']['task_retry_limit'])

    def test_context_checker_encoded(self):
        """Ensure the _context_checker is correctly encoded in options dict."""
        from furious.async import Async
        from furious.async import encode_async_options

        async_job = Async("something", _context_checker=dir)
        options = encode_async_options(async_job)

        self.assertEqual('dir', options['__context_checker'])

    def test_context_checker_encoded_and_decoded(self):
        """Ensure the _context_checker is correctly encoded to and decoded from
        an Async options dict.
        """
        from furious.async import Async

        async_job = Async("something", _context_checker=dir)

        encoded_async = async_job.to_dict()
        self.assertEqual(encoded_async['__context_checker'], 'dir')

        new_async_job = Async.from_dict(encoded_async)
        self.assertEqual(new_async_job.get_options()['_context_checker'], dir)

        self.assertEqual(async_job.to_dict(), new_async_job.to_dict())

    def test_retry_value_is_decodable(self):
        """Ensure that from_dict is the inverse of to_dict when retry options
        are given.
        """
        from furious.async import Async

        async_job = Async("something",
                          task_args={'retry_options': {'task_retry_limit': 5}})
        new_async_job = Async.from_dict(async_job.to_dict())

        self.assertEqual(async_job.to_dict(), new_async_job.to_dict())

    def test_used_async_retry_value_is_decodable(self):
        """Ensure that from_dict is the inverse of to_dict when retry options
        are given and the async has be cast to task.
        """
        from furious.async import Async

        async_job = Async("something",
                          task_args={'retry_options': {'task_retry_limit': 5}})
        async_job.to_dict()

        new_async_job = Async.from_dict(async_job.to_dict())

        self.assertEqual(async_job.to_dict(), new_async_job.to_dict())


class TestAsyncFromOptions(unittest.TestCase):
    """Ensure async_from_options() works correctly."""

    def setUp(self):
        import os
        import uuid

        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    def tearDown(self):
        import os

        del os.environ['REQUEST_ID_HASH']

    def test_no_type(self):
        """Ensure that if not _type is in options, that it defaults to
        furious.async.Async.
        """
        from furious.async import Async
        from furious.async import async_from_options

        async_job = Async(dir)

        options = async_job.to_dict()
        options.pop('_type')

        result = async_from_options(options)

        self.assertIsInstance(result, Async)

    def test_has_type(self):
        """Ensure that if _type is not furious.async.Async that the correct
        subclass is instantiated.
        """
        from furious.async import async_from_options
        from furious.batcher import MessageProcessor

        async_job = MessageProcessor(dir)

        options = async_job.to_dict()

        result = async_from_options(options)

        self.assertIsInstance(result, MessageProcessor)

