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

from mock import Mock
from mock import patch


class TestAutoContextTestCase(unittest.TestCase):
    """Test the AutoContext class."""

    def setUp(self):
        """Setup the test harness and a request hash."""
        import os
        import uuid

        self.harness = testbed.Testbed()
        self.harness.activate()
        self.harness.init_taskqueue_stub()

        # Backup environment
        self._orig_environ = os.environ.copy()
        # Ensure each test looks like it is in a new request.
        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    def tearDown(self):
        """Deactive the test harness, and restore the environment."""
        import os

        self.harness.deactivate()
        os.environ.clear()
        os.environ.update(self._orig_environ)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_add_job_to_context_multiple_batches(self, queue_add_mock):
        """Ensure adding more tasks than the batch_size causes multiple batches
        to get inserted.

        Adding 3 asyncs with a batch_size of 2 should result in two queue.adds,
        containing 2 and 1 task respectively.

        Also ensure that the remaining task is inserted upon exiting the
        context.
        """
        from furious.async import Async
        from furious.context.auto_context import AutoContext

        batch_size = 2

        with AutoContext(batch_size) as ctx:
            # First batch
            job1 = ctx.add('test', args=[1, 2])
            job2 = ctx.add('test2', args=[1, 2])

            # Second batch (not added until "with" section ends))
            job3 = ctx.add('test3', args=[1, 2])

            # Ensure the first two jobs were inserted in the first batch.
            self.assertIsInstance(job1, Async)
            self.assertIsInstance(job2, Async)
            self.assertEqual(1, queue_add_mock.call_count)
            #Ensure only two tasks were inserted
            tasks_added = queue_add_mock.call_args[0][0]
            self.assertEqual(2, len(tasks_added))

        # Ensure the third job was inserted when the context exited.
        self.assertIsInstance(job3, Async)
        # Ensure add has now been called twice.
        self.assertEqual(2, queue_add_mock.call_count)
        # Ensure only one task was inserted
        tasks_added = queue_add_mock.call_args[0][0]
        self.assertEqual(1, len(tasks_added))

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_add_job_to_context_batch_size_unspecified(self, queue_add_mock):
        """When batch_size is None or 0, the default behavior of Context is
        used.  All the tasks are added to the queue when the context is exited.
        """
        from furious.async import Async
        from furious.context.auto_context import AutoContext

        with AutoContext() as ctx:

            job1 = ctx.add('test', args=[1, 2])
            job2 = ctx.add('test2', args=[1, 2])
            job3 = ctx.add('test3', args=[1, 2])

            self.assertIsInstance(job1, Async)
            self.assertIsInstance(job2, Async)
            self.assertIsInstance(job3, Async)

            # Ensure no batches of tasks are inserted yet.
            self.assertFalse(queue_add_mock.called)

        # Ensure the list of tasks added when the context exited.
        self.assertEqual(1, queue_add_mock.call_count)
        # Ensure the three tasks were inserted
        tasks_added = queue_add_mock.call_args[0][0]
        self.assertEqual(3, len(tasks_added))

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_no_jobs(self, queue_add_mock):
        """When no Asyncs are added to the context, ensure that there are no
        errors and nothing is added to the task queue.
        """
        from furious.context.auto_context import AutoContext

        with AutoContext(3):
            pass

        # Ensure queue.add() was never called.
        self.assertEqual(0, queue_add_mock.call_count)
