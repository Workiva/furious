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

from mock import patch

from google.appengine.ext import testbed


class TestContext(unittest.TestCase):
    """Test that the Context object functions in some basic way."""
    def setUp(self):
        harness = testbed.Testbed()
        harness.activate()
        harness.init_taskqueue_stub()

    def test_context_works(self):
        """Ensure using a Context as a context manager works."""
        from furious.context import Context

        with Context():
            pass

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_add_job_to_context_works(self, queue_add_mock):
        """Ensure adding a job works."""
        from furious.async import Async
        from furious.context import Context

        with Context() as ctx:
            job = ctx.add('test', args=[1, 2])

        self.assertIsInstance(job, Async)
        queue_add_mock.assert_called_once()

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_add_multiple_jobs_to_context_works(self, queue_add_mock):
        """Ensure adding multiple jobs works."""
        from furious.context import Context

        with Context() as ctx:
            for _ in range(10):
                ctx.add('test', args=[1, 2])

        queue_add_mock.assert_called_once()
        self.assertEqual(10, len(queue_add_mock.call_args[0][0]))

    @patch('google.appengine.api.taskqueue.Queue', auto_spec=True)
    def test_added_to_correct_queue(self, queue_mock):
        """Ensure jobs are added to the correct queue."""
        from furious.context import Context

        with Context() as ctx:
            ctx.add('test', args=[1, 2], queue='A')
            ctx.add('test', args=[1, 2], queue='A')

        queue_mock.assert_called_once_with(name='A')

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


class TestNew(unittest.TestCase):
    """Test that new returns a new context and adds it to the registry."""

    def test_new(self):
        """Ensure new returns a new context."""
        from furious.context import Context
        from furious.context import new

        self.assertIsInstance(new(), Context)

    def test_new_adds_to_registry(self):
        """Ensure new returns a new context."""
        from furious.context import Context
        from furious.context import _get_local_context
        from furious.context import new

        ctx = new()

        self.assertIsInstance(ctx, Context)
        self.assertIn(ctx, _get_local_context().registry)


class TestInsertTasks(unittest.TestCase):
    """Test that _insert_tasks behaves as expected."""
    def setUp(self):
        harness = testbed.Testbed()
        harness.activate()
        harness.init_taskqueue_stub()

    def test_no_tasks_doesnt_blow_up(self):
        """Ensure calling with an empty list doesn't blow up."""
        from furious.context import _insert_tasks

        _insert_tasks((), 'A')

    @patch('google.appengine.api.taskqueue.Queue', auto_spec=True)
    def test_queue_name_is_honored(self, queue_mock):
        """Ensure the Queue is instantiated with the name."""
        from furious.context import _insert_tasks

        _insert_tasks((None,), 'AbCd')
        queue_mock.assert_called_once_with(name='AbCd')

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_tasks_are_passed_along(self, queue_add_mock):
        """Ensure the list of tasks are passed along."""
        from furious.context import _insert_tasks

        _insert_tasks(('A', 1, 'B', 'joe'), 'AbCd')
        queue_add_mock.assert_called_once_with(('A', 1, 'B', 'joe'),
                                               transactional=False)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_task_add_error(self, queue_add_mock):
        """Ensure an exception doesn't get raised from add."""
        from furious.context import _insert_tasks

        def raise_transient(*args, **kwargs):
            from google.appengine.api import taskqueue
            raise taskqueue.TransientError()

        queue_add_mock.side_effect = raise_transient

        _insert_tasks(('A',), 'AbCd')
        queue_add_mock.assert_called_once_with(('A',), transactional=False)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_batches_get_split(self, queue_add_mock):
        """Ensure a batches get split and retried on errors."""
        from furious.context import _insert_tasks

        def raise_transient(*args, **kwargs):
            from google.appengine.api import taskqueue
            raise taskqueue.TransientError()

        queue_add_mock.side_effect = raise_transient

        _insert_tasks(('A', 1, 'B'), 'AbCd')
        self.assertEqual(5, queue_add_mock.call_count)


class TestJobContext(unittest.TestCase):
    """Test that the Context object functions in some basic way."""
    def setUp(self):
        harness = testbed.Testbed()
        harness.activate()
        harness.init_taskqueue_stub()

    def test_context_requires_async(self):
        """Ensure JobContext requires an async object as its first arg."""
        from furious.context import JobContext

        self.assertRaises(TypeError, JobContext)

    def test_context_works(self):
        """Ensure using a JobContext as a context manager works."""
        from furious.async import Async
        from furious.context import JobContext

        with JobContext(Async(target=dir)):
            pass

    def test_async_is_preserved(self):
        """Ensure JobContext exposes the async as a property."""
        from furious.async import Async
        from furious.context import JobContext

        job = Async(target=dir)

        context = JobContext(job)

        self.assertIs(job, context.async)

    def test_async_is_not_settable(self):
        """Ensure JobContext async can not be set."""
        from furious.async import Async
        from furious.context import JobContext

        job = Async(target=dir)

        context = JobContext(job)

        def set_job():
            context.async = None

        self.assertRaises(AttributeError, set_job)

    def test_job_added_to_local_context(self):
        """Ensure entering the context adds the job to the context stack."""
        from furious.async import Async
        from furious.context import JobContext
        from furious.context import _get_local_context

        job = Async(target=dir)
        with JobContext(job):
            self.assertIn(job, _get_local_context()._executing_async)

    def test_job_removed_from_local_context(self):
        """Ensure exiting the context removes the job from the context stack.
        """
        from furious.async import Async
        from furious.context import JobContext
        from furious.context import _get_local_context

        job = Async(target=dir)
        with JobContext(job):
            pass

        self.assertNotIn(job, _get_local_context()._executing_async)

    def test_job_added_to_end_of_local_context(self):
        """Ensure entering the context adds the job to the end of the job
        context stack.
        """
        from furious.async import Async
        from furious.context import JobContext
        from furious.context import _get_local_context

        job_outer = Async(target=dir)
        job_inner = Async(target=dir)
        with JobContext(job_outer):
            self.assertEqual(1, len(_get_local_context()._executing_async))
            self.assertEqual(job_outer,
                             _get_local_context()._executing_async[-1])

            with JobContext(job_inner):
                self.assertEqual(2, len(_get_local_context()._executing_async))
                self.assertEqual(job_inner,
                                 _get_local_context()._executing_async[-1])

    def test_job_removed_from_end_of_local_context(self):
        """Ensure entering the context removes the job from the end of the job
        context stack.
        """
        from furious.async import Async
        from furious.context import JobContext
        from furious.context import _get_local_context

        job_outer = Async(target=dir)
        job_inner = Async(target=dir)
        with JobContext(job_outer):
            with JobContext(job_inner):
                pass

            self.assertEqual(1, len(_get_local_context()._executing_async))
            self.assertEqual(job_outer,
                             _get_local_context()._executing_async[-1])


