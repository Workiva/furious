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

from mock import call
from mock import Mock
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
    """Ensure tasks from queues are executed."""

    @patch('furious.test_stubs.appengine.queues._execute_task')
    def test_execute_queue(self, _execute_task):
        """When execute_queues is called, ensure tasks are executed, and
        the queue is flushed to remove executed tasks.  Also, ensure True
        is returned since messages were processed.
        """

        from furious.test_stubs.appengine.queues import _execute_queue

        queue_service = Mock()
        queue_service.GetTasks.return_value = ['task1', 'task2', 'task3']

        any_processed = _execute_queue('default', queue_service)

        # Expect _execute_task() to be called for each task
        expected_call_args_list = [call('task1'), call('task2'), call('task3')]

        self.assertEquals(_execute_task.call_args_list,
                          expected_call_args_list)

        # Make sure FlushQueue was called once to clear the queue after
        # tasks were processed
        self.assertEqual(1, queue_service.FlushQueue.call_count)

        # We should have processed tasks, so verify the return value.
        self.assertTrue(any_processed)

    @patch('furious.test_stubs.appengine.queues._execute_task')
    def test_execute_queue_no_tasks(self, _execute_task):
        """When execute_queues is called and there are no tasks in the queue,
        ensure _execute_tasks is not called.
        Ensure False is returned since no messages were processed.
        """

        from furious.test_stubs.appengine.queues import _execute_queue

        queue_service = Mock()
        queue_service.GetTasks.return_value = []

        any_processed = _execute_queue('default', queue_service)

        # Expect _execute_task() to not be called since there are no tasks
        self.assertFalse(_execute_task.called)

        # We should not have processed any tasks, so verify the return value.
        self.assertFalse(any_processed)


class TestExecuteQueues(unittest.TestCase):
    """Ensure tasks from queues are executed."""

    @patch('furious.test_stubs.appengine.queues._execute_queue')
    def test_execute_queues(self, _execute_queue):
        """Ensure all push queues are processed by execute_queues.
        Ensure pull queues are skipped.
        """

        from furious.test_stubs.appengine.queues import execute_queues

        queue_service = Mock()

        queue_descs = [
            {'name': 'default', 'mode': 'push', 'bucket_size': 100},
            {'name': 'default-pull', 'mode': 'pull', 'bucket_size': 5},
            {'name': 'another-pull', 'mode': 'pull', 'bucket_size': 5},
            {'name': 'my_queue', 'mode': 'push', 'bucket_size': 100}]

        # Simulate that messages are processed from each push queue.
        _execute_queue.return_value = True

        any_processed = execute_queues(queue_descs, queue_service)

        # Expected 'default' and 'my_queue' to be the only queues processed
        # since others are pull queues.
        expected_call_args_list = [call('default', queue_service),
                                   call('my_queue', queue_service)]

        # Ensure _execute_queue processes the push queues.
        self.assertEqual(_execute_queue.call_args_list,
                         expected_call_args_list)

        # Make sure that True was returned since messages were processed.
        self.assertTrue(any_processed)

    @patch('furious.test_stubs.appengine.queues._execute_queue')
    def test_execute_queues_no_messages(self, _execute_queue):
        """Ensure the return value is False when no messages are processed from
        the queues.
        Ensure all push queues are processed by execute_queues.
        Ensure pull queues are skipped.
        """

        from furious.test_stubs.appengine.queues import execute_queues

        queue_service = Mock()

        queue_descs = [
            {'name': 'default', 'mode': 'push', 'bucket_size': 100},
            {'name': 'default-pull', 'mode': 'pull', 'bucket_size': 5},
            {'name': 'my_queue', 'mode': 'push', 'bucket_size': 100}]

        # Simulate that there are no messages processed.
        _execute_queue.return_value = False

        any_processed = execute_queues(queue_descs, queue_service)

        # Expect 'default' and 'my_queue' to be processed since the other one
        # is a pull queue.
        expected_call_args_list = [call('default', queue_service),
                                   call('my_queue', queue_service)]

        # Ensure _execute_queue processes tries to process the push queues.
        self.assertEqual(_execute_queue.call_args_list,
                         expected_call_args_list)

        # Make sure that False was returned since messages were not processed.
        self.assertFalse(any_processed)

    @patch('furious.test_stubs.appengine.queues._execute_queue')
    def test_execute_queues_some_queues_with_messages(self, _execute_queue):
        """Ensure that the any_processed return flag is True when the first
        queue processes messages and the next queue does not.
        Ensure all push queues are processed by execute_queues.
        Ensure pull queues are skipped.
        """

        from furious.test_stubs.appengine.queues import execute_queues

        queue_service = Mock()

        queue_descs = [
            {'name': 'default', 'mode': 'push', 'bucket_size': 100},
            {'name': 'my_queue', 'mode': 'push', 'bucket_size': 100}]

        # Simulate that messages were processed from the first push queue,
        # but not the second.
        _execute_queue.side_effect = [True, False]

        any_processed = execute_queues(queue_descs, queue_service)

        # Expected 'default' and 'my_queue' to be processed.
        expected_call_args_list = [call('default', queue_service),
                                   call('my_queue', queue_service)]

        # Ensure _execute_queue processes the push queues.
        self.assertEqual(_execute_queue.call_args_list,
                         expected_call_args_list)

        # Make sure that True was returned since messages were processed.
        self.assertTrue(any_processed)


@attr('slow')
class TestExecuteQueuesIntegration(unittest.TestCase):
    """Ensure tasks from queues are executed.
    """

    def test_execute_queues(self):
        """ """

