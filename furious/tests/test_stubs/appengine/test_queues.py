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

    @patch('time.ctime')
    def test_run_task(self, ctime):
        """When a task is passed to _execute_task, make sure it is run.
        Ensure the task's environment is cleaned up.
        """

        from furious.context import _local
        from furious.test_stubs.appengine.queues import _execute_task

        # Create the async_options to call the target, ctime()
        async_options = {'job': ('time.ctime', None, None)}

        body = base64.b64encode(json.dumps(async_options))

        task = {'body': body, 'headers': ''}

        _execute_task(task)

        # Make sure our function was called
        self.assertTrue(ctime.called)

        # Make sure context cleanup worked
        self.assertFalse('REQUEST_ID_HASH' in os.environ)
        self.assertFalse(hasattr(_local._local_context, 'registry'))

    @patch('time.strftime', autospec=True)
    def test_run_task_with_args_kwargs(self, strftime):
        """When a task with args and kwargs is passed to _execute_task, make
        sure it is run with those parameters.
        Ensure the task's environment is cleaned up.
        """

        from furious.context import _local
        from furious.test_stubs.appengine.queues import _execute_task

        # Create the async_options to call the mocked target, strftime().
        #   To test args and kwargs, our arguments to the mocked strftime
        #   won't match the real strftime's expected parameters.
        args = [1, 2]
        kwargs = {'my_kwarg': 'my_value'}
        async_options = {'job': ('time.strftime',
                                 args, kwargs)}

        body = base64.b64encode(json.dumps(async_options))

        task = {'body': body, 'headers': ''}

        _execute_task(task)

        # Make sure our function was called with the right arguments
        strftime.assert_called_once_with(*args, **kwargs)

        # Make sure context cleanup worked
        self.assertFalse('REQUEST_ID_HASH' in os.environ)
        self.assertFalse(hasattr(_local._local_context, 'registry'))


class TestRunQueue(unittest.TestCase):
    """Ensure tasks from queues are run."""

    @patch('furious.test_stubs.appengine.queues._execute_task')
    def test_run_queue(self, _execute_task):
        """When run() is called, ensure tasks are run, and
        the queue is flushed to remove run tasks.  Also, ensure True
        is returned since messages were processed.
        """

        from furious.test_stubs.appengine.queues import run_queue

        queue_service = Mock()
        queue_service.GetTasks.return_value = ['task1', 'task2', 'task3']

        num_processed = run_queue(queue_service, 'default')

        # Expect _execute_task() to be called for each task
        expected_call_args_list = [call('task1'), call('task2'), call('task3')]

        self.assertEquals(_execute_task.call_args_list,
                          expected_call_args_list)

        # Make sure FlushQueue was called once to clear the queue after
        # tasks were processed
        self.assertEqual(1, queue_service.FlushQueue.call_count)

        # We should have processed tasks, so verify the num processed.
        self.assertEqual(3, num_processed)

    @patch('furious.test_stubs.appengine.queues._execute_task')
    def test_run_queue_no_tasks(self, _execute_task):
        """When run() is called and there are no tasks in the queue,
        ensure _execute_task is not called.
        Ensure False is returned since no messages were processed.
        """

        from furious.test_stubs.appengine.queues import run_queue

        queue_service = Mock()
        queue_service.GetTasks.return_value = []

        num_processed = run_queue(queue_service, 'default')

        # Expect _execute_task() to not be called since there are no tasks
        self.assertFalse(_execute_task.called)

        # We should not have processed any tasks, so verify 0 processed.
        self.assertEqual(0, num_processed)


class TestRunQueues(unittest.TestCase):
    """Ensure tasks from queues are run."""

    @patch('furious.test_stubs.appengine.queues.run_queue')
    def test_run(self, run_queue):
        """Ensure all push queues are processed by run().
        Ensure pull queues are skipped.
        """

        from furious.test_stubs.appengine.queues import run

        queue_descs = [
            {'name': 'default', 'mode': 'push', 'bucket_size': 100},
            {'name': 'default-pull', 'mode': 'pull', 'bucket_size': 5},
            {'name': 'another-pull', 'mode': 'pull', 'bucket_size': 5},
            {'name': 'my_queue', 'mode': 'push', 'bucket_size': 100}]

        queue_service = Mock()
        queue_service.GetQueues.side_effect = [queue_descs]

        # Simulate that messages are processed from each push queue.
        num_in_default = 2
        num_in_my = 1
        # The two zeros are num remaining in the 2nd iteration for each queue.
        run_queue.side_effect = [num_in_default, num_in_my, 0, 0]

        run_result = run(queue_service)

        # Expected 'default' and 'my_queue' to be the only queues processed
        # since others are pull queues.
        expected_call_args_list = [call(queue_service, 'default'),
                                   call(queue_service, 'my_queue'),
                                   call(queue_service, 'default'),
                                   call(queue_service, 'my_queue')]

        # Ensure run_queue processes the push queues.
        self.assertEqual(run_queue.call_args_list, expected_call_args_list)

        # Make sure 2 is returned as the number of messages processed.
        self.assertEqual(num_in_default + num_in_my,
                         run_result['tasks_processed'])
        self.assertEqual(2, run_result['iterations'])

    @patch('furious.test_stubs.appengine.queues.run_queue')
    def test_run_no_messages(self, run_queue):
        """Ensure the return value is False when no messages are processed from
        the queues.
        Ensure all push queues are processed by run().
        Ensure pull queues are skipped.
        """

        from furious.test_stubs.appengine.queues import run

        queue_descs = [
            {'name': 'default', 'mode': 'push', 'bucket_size': 100},
            {'name': 'default-pull', 'mode': 'pull', 'bucket_size': 5},
            {'name': 'my_queue', 'mode': 'push', 'bucket_size': 100}]

        queue_service = Mock()
        queue_service.GetQueues.side_effect = [queue_descs]

        # Simulate that there are no messages processed from any queue.
        run_queue.return_value = 0

        run_result = run(queue_service)

        # Expect 'default' and 'my_queue' to be processed since the other one
        # is a pull queue.
        expected_call_args_list = [call(queue_service, 'default'),
                                   call(queue_service, 'my_queue')]

        # Ensure run_queue processes tries to process the push queues.
        self.assertEqual(run_queue.call_args_list,
                         expected_call_args_list)

        # Make sure that 0 is the number of messages processed.
        self.assertEqual(0, run_result['tasks_processed'])
        self.assertEqual(1, run_result['iterations'])

    @patch('furious.test_stubs.appengine.queues.run_queue')
    def test_run_some_queues_with_messages(self, run_queue):
        """Ensure that the tasks_processed in the return dict is 5 when the
        first queue processes 5 messages and the next queue processes 0.
        Ensure all push queues are processed by run().
        Ensure pull queues are skipped.
        """

        from furious.test_stubs.appengine.queues import run

        queue_descs = [
            {'name': 'default', 'mode': 'push', 'bucket_size': 100},
            {'name': 'my_queue', 'mode': 'push', 'bucket_size': 100}]

        queue_service = Mock(GetQueues=Mock(side_effect=[queue_descs]))

        # Simulate that messages were processed from the first push queue,
        # but not the second.
        run_queue.side_effect = [5, 0, 0, 0]

        run_result = run(queue_service)

        # Expected 'default' and 'my_queue' to be processed.
        # They are processed twice each since messages were processed the
        # first iteration.
        expected_call_args_list = [call(queue_service, 'default'),
                                   call(queue_service, 'my_queue'),
                                   call(queue_service, 'default'),
                                   call(queue_service, 'my_queue')]

        # Ensure run_queue processes the push queues.
        self.assertEqual(run_queue.call_args_list,
                         expected_call_args_list)

        # Make sure that 5 was returned as the number of messages processed.
        self.assertEqual(5, run_result['tasks_processed'])
        self.assertEqual(2, run_result['iterations'])


@attr('slow')
class TestRunQueuesIntegration(unittest.TestCase):
    """Ensure tasks from queues are run."""

    def setUp(self):
        from google.appengine.ext import testbed

        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_taskqueue_stub(root_path="")
        self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

        self.taskqueue_service = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def tearDown(self):
        self.testbed.deactivate()

    @patch('time.ctime')
    def test_run(self, ctime):
        """Ensure tasks are run when run_queues is called."""

        from furious.async import Async
        from furious.test_stubs.appengine.queues import run as run_queues

        # Enqueue a couple of tasks
        async = Async(target='time.ctime')
        async.start()
        async2 = Async(target='time.ctime')
        async2.start()

        # Run the tasks in the queue
        run_queues(self.taskqueue_service)

        self.assertEqual(2, ctime.call_count)


@attr('slow')
class TestPurge(unittest.TestCase):
    """Ensure that purge() clears tasks from queues."""

    def setUp(self):
        from google.appengine.ext import testbed

        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_taskqueue_stub(root_path="")
        self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

        self.taskqueue_service = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def tearDown(self):
        self.testbed.deactivate()

    @patch('time.ctime')
    def test_purge_with_no_tasks(self, ctime):
        """Ensure no errors occur when purging queues containing no tasks.
        Ensure the number of tasks cleared is correct.
        """

        from furious.test_stubs.appengine.queues import purge as purge_queues

        num_cleared = purge_queues(self.taskqueue_service)

        # Ensure zero tasks were cleared.
        self.assertEqual(0, num_cleared)

        # Ensure no tasks were run
        self.assertEqual(0, ctime.call_count)

    @patch('time.ctime')
    def test_purge_with_tasks(self, ctime):
        """After queues are run, ensure no tasks are left to execute.
        Ensure the number of tasks cleared is correct.
        """

        from furious.async import Async
        from furious.batcher import Message
        from furious.test_stubs.appengine.queues import run as run_queues
        from furious.test_stubs.appengine.queues import purge as purge_queues

        # Enqueue a couple of tasks
        async = Async(target='time.ctime')
        async.start()
        async2 = Async(target='time.ctime')
        async2.start()

        Message(queue='default-pull').insert()

        num_cleared = purge_queues(self.taskqueue_service)

        # Run the tasks to check if tasks remain
        run_queues(self.taskqueue_service)

        # Ensure three tasks were cleared, from 'default' and 'default-pull'.
        self.assertEqual(3, num_cleared)

        # Ensure no tasks were run
        self.assertEqual(0, ctime.call_count)

    @patch('time.ctime')
    def test_purge_with_queue_names_provided(self, ctime):
        """When a list of queue_names is provided, ensure purge() clears the
        tasks and none are left to execute.
        Ensure the number of tasks cleared is correct.
        """

        from furious.async import Async
        from furious.batcher import Message
        from furious.test_stubs.appengine.queues import run as run_queues
        from furious.test_stubs.appengine.queues import purge as purge_queues

        # Enqueue a couple of tasks
        async = Async(target='time.ctime')
        async.start()
        async2 = Async(target='time.ctime')
        async2.start()

        Message(queue='default-pull').insert()

        num_cleared = purge_queues(self.taskqueue_service, ['default'])

        # Run the tasks to check if tasks remain
        run_queues(self.taskqueue_service)

        # Ensure two tasks from the default queue were cleared.
        self.assertEqual(2, num_cleared)

        # Ensure no tasks were run
        self.assertEqual(0, ctime.call_count)

    @patch('time.ctime')
    def test_purge_with_string_passed_to_queue_names(self, ctime):
        """If a single queue_name is passed to purge() instead of a list,
        ensure that the queue specified is still cleared.
        Ensure the number of tasks cleared is correct.
        """
        from furious.async import Async
        from furious.batcher import Message
        from furious.test_stubs.appengine.queues import run as run_queues
        from furious.test_stubs.appengine.queues import purge as purge_queues

        # Enqueue a couple of tasks
        async = Async(target='time.ctime')
        async.start()
        async2 = Async(target='time.ctime')
        async2.start()

        # Insert a pull task
        Message(queue='default-pull').insert()

        num_cleared = purge_queues(self.taskqueue_service, 'default')

        # Run the tasks to check if tasks remain
        run_queues(self.taskqueue_service)

        # Ensure two tasks from the default queue were cleared.
        self.assertEqual(2, num_cleared)

        # Ensure no tasks were run
        self.assertEqual(0, ctime.call_count)

    @patch('time.ctime')
    def test_purge_with_nonexistent_queue(self, ctime):
        """If purge is attempted on a queue that does not exist, ensure that an
        Exception is raised.
        """

        from furious.test_stubs.appengine.queues import purge as purge_queues

        self.assertRaises(Exception, purge_queues, self.taskqueue_service,
                          'non-existent-queue')


@attr('slow')
class TestNamesFromQueueService(unittest.TestCase):
    """Ensure that all_queue_names_from_queue_service(),
    pullqueue_names_from_queue_service(), and
    pushqueue_names_from_queue_service() return the correct names.
    """

    def setUp(self):
        from google.appengine.ext import testbed

        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_taskqueue_stub(root_path="")
        self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

        self.taskqueue_service = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def tearDown(self):
        self.testbed.deactivate()

    @patch('time.ctime')
    def test_pullqueue_names_from_queue_service(self, ctime):
        """Ensure the correct pull queue names are returned from
        pullqueue_names_from_queue_service.
        """

        from furious.test_stubs.appengine.queues import (
            pullqueue_names_from_queue_service)

        names = pullqueue_names_from_queue_service(self.taskqueue_service)

        self.assertEqual(names, ['default-pull'])

    @patch('time.ctime')
    def test_pushqueue_names_from_queue_service(self, ctime):
        """Ensure the correct push queue names are returned from
        pushqueue_names_from_queue_service.
        """

        from furious.test_stubs.appengine.queues import (
            pushqueue_names_from_queue_service)

        names = pushqueue_names_from_queue_service(self.taskqueue_service)

        self.assertEqual(names, ['default'])

    @patch('time.ctime')
    def test_all_queue_names_from_queue_service(self, ctime):
        """Ensure the correct queue names are returned from
        all_queue_names_from_queue_service.
        """

        from furious.test_stubs.appengine.queues import (
            all_queue_names_from_queue_service)

        names = all_queue_names_from_queue_service(self.taskqueue_service)

        self.assertEqual(names, ['default', 'default-pull'])


@attr('slow')
class TestGetTasks(unittest.TestCase):
    """Ensure that get_tasks(), returns the queues' tasks."""

    def setUp(self):
        from google.appengine.ext import testbed

        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_taskqueue_stub(root_path="")
        self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

        self.queue_service = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def tearDown(self):
        self.testbed.deactivate()

    def test_get_tasks_when_there_are_no_tasks(self):
        """Ensure that no tasks are returned from get_tasks() when no tasks
        have been added yet.
        """

        from furious.test_stubs.appengine.queues import get_tasks

        task_dict = get_tasks(self.queue_service)
        num_tasks = sum([len(task_list) for task_list in task_dict.values()])

        self.assertEqual(0, num_tasks)

    @patch('time.ctime')
    def test_get_tasks_from_all_queues(self, ctime):
        """Ensure all tasks are returned from get_tasks()."""

        from furious.async import Async
        from furious.batcher import Message
        from furious.test_stubs.appengine.queues import get_tasks

        # Enqueue a couple of tasks
        async = Async(target='time.ctime')
        async.start()
        async2 = Async(target='time.ctime')
        async2.start()

        # Insert a pull task
        Message(queue='default-pull').insert()

        task_dict = get_tasks(self.queue_service)
        num_tasks = sum([len(task_list) for task_list in task_dict.values()])

        self.assertEqual(3, num_tasks)

    @patch('time.ctime')
    def test_get_tasks_when_queue_names_are_specified(self, ctime):
        """Ensure queues' tasks are returned from get_tasks() when a list of
        queue_names are passed as an argument.
        """

        from furious.async import Async
        from furious.batcher import Message
        from furious.test_stubs.appengine.queues import get_tasks

        # Enqueue a couple of tasks
        async = Async(target='time.ctime')
        async.start()
        async2 = Async(target='time.ctime')
        async2.start()

        # Insert a pull task
        Message(queue='default-pull').insert()

        task_dict = get_tasks(self.queue_service, ['default'])
        num_tasks = sum([len(task_list) for task_list in task_dict.values()])

        self.assertEqual(2, num_tasks)

    @patch('time.ctime')
    def test_get_tasks_when_queue_name_string_is_passed(self, ctime):
        """Ensure a queue's tasks are returned from get_tasks() when a
        queue_name is passed as a string.
        """

        from furious.async import Async
        from furious.batcher import Message
        from furious.test_stubs.appengine.queues import get_tasks

        # Enqueue a couple of tasks
        async = Async(target='time.ctime')
        async.start()
        async2 = Async(target='time.ctime')
        async2.start()

        # Insert a pull task
        Message(queue='default-pull').insert()

        task_dict = get_tasks(self.queue_service, 'default-pull')
        num_tasks = sum([len(task_list) for task_list in task_dict.values()])

        self.assertEqual(1, num_tasks)

    @patch('time.ctime')
    def test_get_tasks_with_nonexistent_queue(self, ctime):
        """If a non-existing queue is passed to get_tasks(), ensure that an
        Exception is raised.
        """
        from furious.test_stubs.appengine.queues import get_tasks

        self.assertRaises(Exception, get_tasks, self.queue_service,
                          'non-existent-queue')
