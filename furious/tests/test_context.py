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

    def test_context_works(self):
        """Ensure using a Context as a context manager works."""
        from furious.context import Context

        with Context():
            pass

    def test_add_job_to_context_works(self):
        """Ensure adding a job works."""
        from furious.async import Async
        from furious.context import Context

        with Context() as ctx:
            job = ctx.add('test', args=[1, 2])

        self.assertIsInstance(job, Async)


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
        from furious.context import _local_context
        from furious.context import new

        ctx = new()

        self.assertIsInstance(ctx, Context)
        self.assertIn(ctx, _local_context.registry)


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

