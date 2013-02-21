"""
Retrieve app engine tasks from testbed queues and run them.

The purpose is to run full local integration tests with appengine testbed.
"""

import base64
import os
import uuid

from _local import _clear_context
from ..handlers import process_async_task


def _execute_task(task):
    """Extract the body and header in the task and execute the task."""

    # Ensure each test looks like it is in a new request.
    os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    body = base64.b64decode(task['body'])
    return_code, func_path = process_async_task(task['headers'], body)

    # TODO: Possibly do more with return_codes

    # Cleanup context since we will be executing more tasks in this process.
    _clear_context()

    # cleanup
    del os.environ['REQUEST_ID_HASH']


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

        any_processed |= _execute_queue(queue_desc['name'], queue_service)

    return any_processed
