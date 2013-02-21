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

import base64
import json
import os
import unittest

from nose.plugins.attrib import attr

from mock import patch


class TestExecuteTask(unittest.TestCase):
    """Ensure _execute_task runs the tasks."""

    @patch('furious.batcher.Message')
    def test_execute_task(self, Message):
        """When a task is passed to _execute_task, make sure it is executed,
        and the task's environment is cleaned up."""

        from furious.context import _local
        from furious.test_stubs.appengine.queues import _execute_task

        # Create the async_options to call the dir function
        async_options = {'job': ('furious.batcher.Message', None, None)}

        body = base64.b64encode(json.dumps(async_options))

        task = {'body': body, 'headers': ''}

        _execute_task(task)

        # Make sure our function was called
        self.assertTrue(Message.called)

        # Make sure cleanup worked
        self.assertFalse('REQUEST_ID_HASH' in os.environ)
        self.assertFalse(hasattr(_local._local_context, 'registry'))


class TestExecuteQueue(unittest.TestCase):
    """Ensure tasks from queues are executed.
    """

    def test_execute_queue(self):
        """ """


class TestExecuteQueues(unittest.TestCase):
    """Ensure tasks from queues are executed.
    """

    def test_execute_queues(self):
        """ """


@attr('slow')
class TestExecuteQueues(unittest.TestCase):
    """Ensure tasks from queues are executed.
    """

    def test_execute_queues(self):
        """ """

