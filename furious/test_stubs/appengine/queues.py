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

"""
Retrieve app engine tasks from testbed queues and run them.

The purpose is to run full local integration tests with the App Engine testbed.

Advanced app engine features such as automatic retries are not implemented.


Examples:

# See integration test for more detailed taskq service setup.
taskq_service = testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)


# Run all tasks in all queues until they are empty.
run(taskq_service)


# Run all tasks in all queues until empty or until 5 iterations is reached.
run(taskq_service, max_iterations=5)


# Run all tasks from selected queues until they are empty.
run(taskq_service, ["queue1", "queue2"])


# Setup state for running multiple times.
runner = Runner(taskq_service)
runner.run()
...
runner.run()

"""

import base64
from collections import defaultdict
import datetime
import logging
import os
import random
import time
import traceback
import uuid

from google.appengine.api import taskqueue
from google.appengine.api.apiproxy_stub_map import apiproxy
from google.appengine.api.taskqueue import taskqueue_stub

from furious.context._local import _clear_context
from furious.handlers import process_async_task

__all__ = ['run', 'run_queue', 'Runner', 'add_tasks', 'get_tasks',
           'purge_tasks', 'get_queue_names', 'get_pull_queue_names',
           'get_push_queue_names']


def run_queue(taskq_service, queue_name, non_furious_url_prefixes=None,
              non_furious_handler=None, enable_retries=False):
    """Get the tasks from a queue.  Clear the queue, and run each task.

    If tasks are reinserted into this queue, this function needs to be called
    again for them to run.

    :param taskqueue_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param queue_name: :class: `str`
    :param non_furious_url_prefixes: :class: `list` of url prefixes that the
                                 furious task runner will run.
    :param non_furious_handler: :class: `func` handler for non furious tasks to
                                run within.
    :type enable_retries: bool Whether to enable task retries.
    """

    if enable_retries:
        return run_queue_enable_retries(
            taskq_service, queue_name, non_furious_url_prefixes=None,
            non_furious_handler=None)

    # Get tasks and clear them
    tasks = taskq_service.GetTasks(queue_name)

    taskq_service.FlushQueue(queue_name)

    num_processed = 0

    for task in tasks:
        _execute_task(task, non_furious_url_prefixes, non_furious_handler)

        num_processed += 1

    return num_processed


def run_queue_enable_retries(
        queue_service, queue_name, non_furious_url_prefixes=None,
        non_furious_handler=None):
    """
    Run the tasks in a queue with AE task retries and retry limit.

    Ignores delays to start tasks.
    Only deletes the task if task doesn't raise an exception or it has
    reached the max number of retries.
    Uses internal App Engine SDK calls, so run_queue() is be more
    stable if retries are not important.

    :param queue_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param queue_name: :class: `str`
    :param non_furious_url_prefixes: :class: `list` of url prefixes that the
     furious task runner will run.
    :param non_furious_handler: :class: `func` handler for non furious tasks to
     run within.
    """

    queue = queue_service._GetGroup().GetQueue(queue_name)
    task_responses = [task_response for (_, _, task_response)
                      in queue._sorted_by_eta]

    num_processed = 0

    for task_response in task_responses:

        now = datetime.datetime.utcnow()
        task_dict = taskqueue_stub.QueryTasksResponseToDict(
            queue_name, task_response, now)

        success = _execute_wrapped_task(
            task_dict, queue_service, queue_name,
            non_furious_url_prefixes, non_furious_handler)

        if success:
            queue_service.DeleteTask(queue_name, task_dict.get('name'))
        else:
            _retry_or_delete(
                task_dict, task_response, queue, queue_service, queue_name)

        num_processed += 1

    return num_processed


def _retry_or_delete(task, task_response, queue, queue_service, queue_name):
    """
    Increment task retry count or delete if it has reached max retries.

    :type task: dict Task as a dictionary.
    :param task_response: :class:
     `taskqueue.taskqueue_service_pb.TaskQueueQueryTasksResponse_Task`
    :param queue: :class:
     `google.appengine.api.taskqueue.taskqueue_stub._Queue`
    :param queue_service: :class:
     `google.appengine.api.taskqueue_stub.TaskQueueServiceStub`
    :type queue_name: str The name of the queue
    """

    # See if task can retry, delete otherwise.
    retry = taskqueue_stub.Retry(task_response, queue)
    if retry.CanRetry(task_response.retry_count() + 1, 0):
        # Increment retries.  We ignore the delay when running tasks.
        now_eta_usec = time.time() * 1e6
        eta_usec = task.get('eta_usec') or now_eta_usec
        queue.PostponeTask(task_response, eta_usec + 10)
    else:
        # Can't retry, likely reached the max retry count.
        queue_service.DeleteTask(queue_name, task.get('name'))


def run(taskq_service=None, queue_names=None, max_iterations=None,
        non_furious_url_prefixes=None, non_furious_handler=None,
        enable_retries=False):
    """
    Run all the tasks in queues, limited by max_iterations.

    An 'iteration' processes at least all current tasks in the queues.
    If any tasks insert additional tasks into a queue that has already
    been processed, at least one more iteration is needed.

    :param taskq_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param queue_names: :class: `list` of queue name strings.
    :param max_iterations: :class: `int` maximum number of iterations to run.
    :param non_furious_url_prefixes: :class: `list` of url prefixes that the
                                 furious task runner will run.
    :param non_furious_handler: :class: `func` handler for non furious tasks to
                                run within.
    :type enable_retries: bool Whether to enable task retries.
    """
    if not taskq_service:
        taskq_service = apiproxy.GetStub('taskqueue')

    if not queue_names:
        queue_names = get_push_queue_names(taskq_service)

    iterations = 0
    tasks_processed = 0
    processed = (max_iterations is None or max_iterations > 0)

    # Keep processing if we have processed any tasks and are under our limit.
    while processed:

        processed = _run(taskq_service, queue_names, non_furious_url_prefixes,
                         non_furious_handler, enable_retries)
        tasks_processed += processed
        iterations += 1

        if max_iterations and iterations >= max_iterations:
            break

    return {'iterations': iterations, 'tasks_processed': tasks_processed}


def get_tasks(taskq_service, queue_names=None):
    """
    Get all tasks from queues and return them in a dict keyed by queue_name.
    If queue_names not specified, returns tasks from all queues.

    :param taskq_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param queue_names: :class: `list` of queue name strings.
    """

    # Make sure queue_names is a list
    if isinstance(queue_names, basestring):
        queue_names = [queue_names]

    if not queue_names:
        queue_names = get_queue_names(taskq_service)

    task_dict = defaultdict(list)

    for queue_name in queue_names:
        # Get tasks
        tasks = taskq_service.GetTasks(queue_name)

        task_dict[queue_name].extend(tasks)

    return task_dict


def add_tasks(taskq_service, task_dict):
    """
    Allow readding of multiple tasks across multiple queues.
    The task_dict is a dictionary with tasks for each queue, keyed by queue
    name.
    Tasks themselves can be dicts like those received from GetTasks() or Task
    instances.

    :param taskq_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param queue_names: :class: `dict` of queue name: tasks dictionary.
    """

    num_added = 0

    # Get the descriptions so we know when to specify PULL mode.
    queue_descriptions = taskq_service.GetQueues()
    queue_desc_dict = dict((queue_desc['name'], queue_desc)
                           for queue_desc in queue_descriptions)

    # Loop over queues and add tasks for each.
    for queue_name, tasks in task_dict.items():

        queue = taskqueue.Queue(queue_name)

        is_pullqueue = ('pull' == queue_desc_dict[queue_name]['mode'])

        tasks_to_add = []

        # Ensure tasks are formatted to add to queues.
        for task in tasks:

            # If already formatted as a Task, add it.
            if isinstance(task, taskqueue.Task):
                tasks_to_add.append(task)
                continue

            # If in dict format that comes from GetTasks(), format it as a Task
            # First look for payload.  If no payload, look for body to decode.
            if 'payload' in task:
                payload = task['payload']
            else:
                payload = base64.b64decode(task.get('body'))

            # Setup different parameters for pull and push queues
            if is_pullqueue:
                task_obj = taskqueue.Task(payload=payload,
                                          name=task.get('name'),
                                          method='PULL',
                                          url=task.get('url'))
            else:
                task_obj = taskqueue.Task(payload=payload,
                                          name=task.get('name'),
                                          method=task.get('method'))

            tasks_to_add.append(task_obj)

        # Add tasks to queue
        if tasks_to_add:
            queue.add(tasks_to_add)
            num_added += len(tasks_to_add)

    return num_added


def purge_tasks(taskq_service, queue_names=None):
    """Remove all tasks from queues.

    :param taskq_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param queue_names: :class: `list` of queue name strings.
    """

    # Make sure queue_names is a list
    if isinstance(queue_names, basestring):
        queue_names = [queue_names]

    if not queue_names:
        queue_names = get_queue_names(taskq_service)

    num_tasks = 0

    for queue_name in queue_names:
        # Get tasks to help give some feedback
        tasks = taskq_service.GetTasks(queue_name)
        num_tasks += len(tasks)

        taskq_service.FlushQueue(queue_name)

    return num_tasks


def get_queue_names(taskq_service, mode=None):
    """Returns push queue names from the Task Queue service.

    :param taskq_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param mode: :class: `str` Task queue mode "pull" or "push"
    """

    queue_descriptions = taskq_service.GetQueues()

    return [description['name']
            for description in queue_descriptions]


def get_pull_queue_names(taskq_service, mode=None):
    """Returns pull queue names from the Task Queue service.

    :param taskq_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param mode: :class: `str` Task queue mode "pull" or "push"
    """

    queue_descriptions = taskq_service.GetQueues()

    return [description['name']
            for description in queue_descriptions
            if 'pull' == description.get('mode')]


def get_push_queue_names(taskq_service, mode=None):
    """Returns push queue names from the Task Queue service.

    :param taskq_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param mode: :class: `str` Task queue mode "pull" or "push"
    """

    queue_descriptions = taskq_service.GetQueues()

    return [description['name']
            for description in queue_descriptions
            if 'push' == description.get('mode')]


class Runner(object):
    """A class to help run pull queues.

    Allows parameters such as taskq_service and queue_names be specified at
    __init__ instead of in each run() call.
    """
    # TODO: WRITE UNIT TESTS FOR ME.

    def __init__(self, taskq_service, queue_names=None):
        """Store taskq_service and optionally queue_name list for reuse."""

        self.taskq_service = taskq_service

        if None == queue_names:
            self.queue_names = get_push_queue_names(self.taskq_service)
        else:
            self.queue_names = queue_names

    def run(self, max_iterations=None):
        """Run the existing tasks for all pushqueue."""

        return run(self.taskq_service, self.queue_names, max_iterations)

    def run_queue(self, queue_name):
        """Run all the existing tasks for one queue."""

        return run_queue(self.taskq_service, queue_name)


def _execute_wrapped_task(task_dict, queue_service, queue_name,
                          non_furious_url_prefixes=None,
                          non_furious_handler=None):
    """
    Wrap _execute_task in a try/except.

    Also, set number of retries in the environment.
    Return True if task executes normally, False if it fails.
    """

    # Set retry count in environment.
    header_dict = dict(task_dict['headers'])
    os.environ['HTTP_X_APPENGINE_TASKRETRYCOUNT'] = header_dict.get(
        'X-AppEngine-TaskRetryCount', '0')

    success = False
    try:
        _execute_task(
            task_dict, non_furious_url_prefixes=non_furious_url_prefixes,
            non_furious_handler=non_furious_handler)
        success = True

    except Exception, e:
        # Tests for Abort don't allow logging.exception here, so only log as a
        # warning and include the traceback.
        stack = traceback.format_exc()
        logging.warning("%s\n%s" % (repr(e), repr(stack)))

    del os.environ['HTTP_X_APPENGINE_TASKRETRYCOUNT']

    return success


def _execute_task(task, non_furious_url_prefixes=None,
                  non_furious_handler=None):
    """Extract the body and header from the task and process it.

    :param task: :class: `taskqueue.Task`
    :param non_furious_url_prefixes: :class: `list` of url prefixes that the
                                 furious task runner will run.
    :param non_furious_handler: :class: `func` handler for non furious tasks to
                                run within.
    """
    if not _is_furious_task(task, non_furious_url_prefixes,
                            non_furious_handler):
        return

    # Ensure each test looks like it is in a new request.
    os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    # Decode the body and process the task.
    body = base64.b64decode(task['body'])

    return_code, func_path = process_async_task(dict(task['headers']), body)

    # TODO: Possibly do more with return_codes.

    # Cleanup context since we will be executing more tasks in this process.
    _clear_context()

    del os.environ['REQUEST_ID_HASH']


def _is_furious_task(task, non_furious_url_prefixes=None,
                     non_furious_handler=None):
    """Return flag if task is a Furious task. If no non_furious_url_prefixes is
    passed in the task will be flagged as Furious. Otherwise it will compare
    the task url to the furious_url_prefix. If not a Furious task it will try
    to run it within the non_furious_handler if one is handed in.

    :param task: :class: `taskqueue.Task`
    :param non_furious_url_prefixes: :class: `list` of url prefixes that the
                                 furious task runner will run.
    :param non_furious_handler: :class: `func` handler for non furious tasks to
                                run within.
    """
    if not non_furious_url_prefixes:
        return True

    task_url = task['url']

    for non_furious_url_prefix in non_furious_url_prefixes:
        if task_url.startswith(non_furious_url_prefix):
            if non_furious_handler:
                logging.info("Passing %s to non Furious handler %s", task,
                             non_furious_handler)
                non_furious_handler(task)

            return False

    return True


def _run(taskq_service, queue_names, non_furious_url_prefixes=None,
         non_furious_handler=None, enable_retries=False):
    """Run individual tasks in push queues.

    :param taskq_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param queue_names: :class: `list` of queue name strings
    :param non_furious_url_prefixes: :class: `list` of url prefixes that the
                                 furious task runner will run.
    :param non_furious_handler: :class: `func` handler for non furious tasks to
                                run within.
    :type enable_retries: bool Whether to enable task retries.
    """

    num_processed = 0

    # Process each queue
    # TODO: Round robin instead of one queue at a time.
    for queue_name in queue_names:
        num_processed += run_queue(taskq_service, queue_name,
                                   non_furious_url_prefixes,
                                   non_furious_handler, enable_retries)

    return num_processed


def run_random(queue_service, queues, random_seed=123, max_tasks=100):
    """Run individual tasks in push queues randomly.  This will run by
    randomly picking a queue and then randomly picking a task from that queue.
    Once the task is ran we pick a different random queue and random task
    until we've either exhausted the queues or we have ran the 'max_tasks'.

    This will only run tasks from 'push' queues.

    :param queue_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param queues: :class: `dict` of queue 'descriptions' where each
        description is a 'dict' with at least 'mode' and 'name'.
    :param random_seed: :class: 'int' to be used to seed random
    :param max_tasks: :class: 'int' to indicate the max number of tasks
        to be ran before we exit.
    :returns: :class: 'int' with count of tasks ran
    """
    if not queues:
        return 0

    queue_count = len(queues)

    random.seed(random_seed)

    # Run until we hit the task limit
    num_processed = 0
    while num_processed < max_tasks:

        # Start by grabbing a random queue to run from
        queue_index = random.randrange(queue_count)

        # Grab the queue description
        queue_desc = queues[queue_index]
        processed_queue_count = 0

        # Keep trying to run a task until we either successfully run or
        # we have ran through all of the queues.
        task_ran = False
        while not task_ran and processed_queue_count < queue_count:

            # Only process from push queues.
            if queue_desc.get('mode') == 'push':
                queue_name = queue_desc['name']
                task_ran = _run_random_task_from_queue(queue_service, queue_name)

            if task_ran:
                num_processed += 1
            else:
                # There isn't a task for the queue so we need to check the next
                # queue in the list.
                queue_index += 1
                queue_desc = queues[queue_index % queue_count]
                processed_queue_count += 1

        # If we ran through all of the queues without running a task then
        # we can break out
        if not task_ran:
            break

    return num_processed


def _run_random_task_from_queue(queue_service, queue_name):
    """Attempts to run a random task from the queue identified
    by queue_name.  Returns True if a task was ran otherwise
    returns False.

    :param queue_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param queue_name: :class: `basestring` with name of queue to run from.
    :returns: :class: 'bool' true if we ran a task; false otherwise
    """
    task = _fetch_random_task_from_queue(queue_service, queue_name)

    if task and task.get('name'):
        _execute_task(task)

        queue_service.DeleteTask(queue_name, task.get('name'))

        return True

    return False


def _fetch_random_task_from_queue(queue_service, queue_name):
    """Returns a random task description from the queue identified by queue_name
    if there exists at least one task in the queue.

    :param queue_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param queue_name: :class: `basestring` with name of queue to pull from.
    :returns: :class: 'dict' containing the task description to run.
    """
    tasks = queue_service.GetTasks(queue_name)

    if not tasks:
        return None

    task = random.choice(tasks)

    if not task:
        return None

    return task


### Deprecated ###

def execute_queues(queues, queue_service):
    """ DEPRECATED
    Remove this as soon as references to this in other libraries are gone.
    Use run() or Runner.run() instead of this.

    Run individual tasks in push queues.
    """
    import logging
    logging.warning('This method is deprecated, switch to ')

    num_processed = False

    # Process each queues
    for queue_desc in queues:

        # Don't pull anything from pull queues.
        if queue_desc.get('mode') == 'pull':
            continue

        num_processed = (run_queue(queue_service, queue_desc['name'])
                         or num_processed)

    return bool(num_processed)
