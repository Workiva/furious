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
"""

import base64
import os
import uuid

from furious.context._local import _clear_context
from furious.handlers import process_async_task


def _execute_task(task):
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
