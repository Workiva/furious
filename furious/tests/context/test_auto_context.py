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
    def test_add_job_to_context_works(self, queue_add_mock):
        """Ensure adding a job works."""
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
            queue_add_mock.assert_called_once()
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
