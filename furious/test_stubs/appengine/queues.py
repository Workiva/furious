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
result = run(taskq_service)


# Run all tasks in all queues until empty or until 5 iterations is reached.
result = run(taskq_service, max_iterations=5)


# Run all tasks from selected queues until they are empty.
result = run(taskq_service, ["queue1", "queue2"])


# Setup state for running multiple times.
runner = Runner(taskq_service)
runner.run()
...
runner.run()

"""

import base64
import os
import uuid

from furious.context._local import _clear_context
from furious.handlers import process_async_task


def _run_task(task):
    """Extract the body and header from the task and process it."""

    # Ensure each test looks like it is in a new request.
    os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    # Decode the body and process the task.
    body = base64.b64decode(task['body'])
    return_code, func_path = process_async_task(task['headers'], body)

    # TODO: Possibly do more with return_codes.

    # Cleanup context since we will be executing more tasks in this process.
    _clear_context()
    del os.environ['REQUEST_ID_HASH']


def run_queue(taskq_service, queue_name):
    """Get the tasks from a queue.  Clear the queue, and run each task.

    If tasks are reinserted into this queue, this function needs to be called
    again for them to run.
    """

    # Get tasks and clear them
    tasks = taskq_service.GetTasks(queue_name)

    taskq_service.FlushQueue(queue_name)

    num_processed = 0

    for task in tasks:
        _run_task(task)

        num_processed += 1

    return num_processed


def _run(taskq_service, queue_names):
    """Run individual tasks in push queues.

    :param taskq_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param queue_names: :class: `list` of queue name strings
    """

    num_processed = 0

    # Process each queue
    # TODO: Round robin instead of one queue at a time.
    for queue_name in queue_names:
        num_processed += run_queue(taskq_service, queue_name)

    return num_processed


def run(taskq_service, queue_names=None, max_iterations=None):
    """
    Run all the tasks in queues, limited by max_iterations.

    An 'iteration' processes at least all current tasks in the queues.
    If any tasks insert additional tasks into a queue that has already
    been processed, at least one more iteration is needed.

    :param taskq_service: :class: `taskqueue_stub.TaskQueueServiceStub`
    :param queue_names: :class: `list` of queue name strings.
    :param max_iterations: :class: `int` maximum number of iterations to run.
    """

    if not queue_names:
        queue_names = pullqueue_names_from_taskq_service(taskq_service)

    iterations = 0
    tasks_processed = 0
    processed = 1

    # Don't check iteration count if max_iterations is None.
    dont_check_iterations = (max_iterations is None)

    # Keep processing if we have processed any tasks and are under our limit.
    while processed and (dont_check_iterations or iterations < max_iterations):

        processed = _run(taskq_service, queue_names)
        tasks_processed += processed
        iterations += 1

    return {'iterations': iterations, 'tasks_processed': tasks_processed}


def pullqueue_names_from_taskq_service(taskq_service):
    """Returns push queue names from the taskqueue service."""

    queue_descs = taskq_service.GetQueues()

    return [queue_desc['name']
            for queue_desc in queue_descs
            if 'pull' != queue_desc['mode']]


class Runner:
    """A class to help run pull queues.

    Allows parameters such as taskq_service and queue_names be specified at
    __init__ instead of in each run() call.
    """

    def __init__(self, taskq_service, queue_names=None):
        """Store taskq_service and optionally queue_name list for reuse."""

        self.taskq_service = taskq_service

        if None == queue_names:
            self.queue_names = pullqueue_names_from_taskq_service(
                self.taskq_service)
        else:
            self.queue_names = queue_names

    def run(self, max_iterations=None):
        """Run the existing tasks for all pushqueue."""

        return run(self.taskq_service, self.queue_names, max_iterations)

    def run_queue(self, queue_name):
        """Run all the existing tasks for one queue."""

        return run_queue(self.taskq_service, queue_name)


### Deprecated ###

def execute_queues(queues, queue_service):
    """ DEPRECATED
    Remove this as soon as references to this in other libraries are gone.
    Use run() or Runner.run() instead of this.

    Run individual tasks in push queues.
    """

    num_processed = False

    # Process each queues
    for queue_desc in queues:

        # Don't pull anything from pull queues.
        if queue_desc.get('mode') == 'pull':
            continue

        num_processed = (run_queue(queue_service, queue_desc['name'])
                         or num_processed)

    return bool(num_processed)
