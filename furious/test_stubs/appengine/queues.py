"""
Retrieve app engine tasks from testbed queues and run them.

The purpose is to run full local integration tests with the App Engine testbed.

Advanced app engine features such as automatic retries are not implemented.
"""

import base64
import copy
import os
import uuid

from furious.context._local import _clear_context
from furious.handlers import process_async_task


class EnvModifiedException(Exception):
    """Indicates an environment has been modified and not returned to its
    original state.
    """


def _execute_task(task):
    """Extract the body and header from the task and process it.

    Raise an exception if the task leaves modifications to the environment.
    """

    # Ensure each test looks like it is in a new request.
    os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    # Make a copy of the environment so we can ensure it's not changed later.
    orig_environ = copy.deepcopy(os.environ)

    # Decode the body and process the task.
    body = base64.b64decode(task['body'])
    return_code, func_path = process_async_task(task['headers'], body)

    # TODO: Possibly do more with return_codes.

    # Ensure users have not modified the execution environment.
    modified_environment = False

    if not (os.environ == orig_environ):
        modified_environment = True
        env_error_msg = ("Environment was modified by task and not restored\n"
                         + "orig env: %s\n\n, current env: %s" %
                         (orig_environ, os.environ))

    # Cleanup context since we will be executing more tasks in this process.
    _clear_context()
    del os.environ['REQUEST_ID_HASH']

    # Now that our environment changes are cleaned up, raise any exceptions.
    if modified_environment:
        raise EnvModifiedException(env_error_msg)


def _execute_queue(queue_name, queue_service):
    """Get the tasks from a queue.  Clear the queue, and run each task."""

    # Get tasks and clear them
    tasks = queue_service.GetTasks(queue_name)

    queue_service.FlushQueue(queue_name)

    any_processed = False

    for task in tasks:
        _execute_task(task)

        any_processed = True

    return any_processed


def execute_queues(queues, queue_service):
    """Run individual tasks in push queues."""

    any_processed = False

    # Process each queues
    for queue_desc in queues:

        # Don't pull anything from pull queues.
        if queue_desc.get('mode') == 'pull':
            continue

        any_processed = (_execute_queue(queue_desc['name'], queue_service)
                         or any_processed)

    return any_processed
