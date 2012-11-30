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
Furious context may be used to group a collection of Async tasks together.

Usage:


    with context.new() as current_context:
        # An explicitly constructed Async object may be passed in.
        async_job = Async(some_function,
                          [args, for, other],
                          {'kwargs': 'for', 'other': 'function'},
                          queue='workgroup')
        current_context.add(async_job)

        # Alternatively, add will construct an Async object when given
        # a function path or reference as the first argument.
        async_job = current_context.add(
            another_function,
            [args, for, other],
            {'kwargs': 'for', 'other': 'function'},
            queue='workgroup')

"""

import os
import threading

from google.appengine.api import taskqueue

# NOTE: Do not import this directly.  If you MUST use this, access it
# through _get_local_context.
_local_context = None


class NotInContextError(Exception):
    """Call that requires context made outside context."""


class ContextExistsError(Exception):
    """Call made within context that should not be."""


class AlreadyInContextError(Exception):
    """Attempt to set context on an Async that is already executing in a
    context.
    """


def new():
    """Get a new furious context and add it to the registry."""
    _init()
    new_context = Context()
    _local_context.registry.append(new_context)
    return new_context


def init_job_context_with_async(async):
    """Instantiate a new JobContext and store a reference to it in the global
    async context to make later retrieval easier.
    """
    _init()

    if _local_context._executing_async_context:
        raise ContextExistsError

    job_context = JobContext(async)
    _local_context._executing_async_context = job_context
    return job_context


def get_current_async():
    """Return a reference to the currently executing Async job object
    or None if not in an Async job.
    """
    _init()
    if _local_context._executing_async:
        return _local_context._executing_async[-1]

    raise NotInContextError('Not in an executing JobContext.')


def _insert_tasks(tasks, queue, transactional=False):
    """Insert a batch of tasks into the specified queue. If an error occurs
    during insertion, split the batch and retry until they are successfully
    inserted.
    """
    if not tasks:
        return

    try:
        taskqueue.Queue(name=queue).add(tasks, transactional=transactional)
    except (taskqueue.TransientError,
            taskqueue.TaskAlreadyExistsError,
            taskqueue.TombstonedTaskError):
        count = len(tasks)
        if count <= 1:
            return

        _insert_tasks(tasks[:count / 2], queue, transactional)
        _insert_tasks(tasks[count / 2:], queue, transactional)


class Context(object):
    """Furious context object.

    NOTE: Use the module's new function to get a context, do not manually
    instantiate.
    """
    def __init__(self, insert_tasks=_insert_tasks):
        self._tasks = []
        self.insert_tasks = insert_tasks

        _init()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._tasks:
            self._handle_tasks()

        return False

    def _handle_tasks(self):
        """Convert all Async's into tasks, then insert them into queues."""
        task_map = self._get_tasks_by_queue()
        for queue, tasks in task_map.iteritems():
            self.insert_tasks(tasks, queue=queue)

    def _get_tasks_by_queue(self):
        """Return the tasks for this Context, grouped by queue."""
        task_map = {}
        for async in self._tasks:
            queue = async.get_queue()
            task = async.to_task()
            task_map.setdefault(queue, []).append(task)
        return task_map

    def add(self, target, args=None, kwargs=None, **options):
        """Add an Async job to this context.

        Takes an Async object or the argumnets to construct an Async
        object as argumnets.  Returns the newly added Async object.
        """
        from .async import Async
        if not isinstance(target, Async):
            target = Async(target, args, kwargs, **options)

        self._tasks.append(target)
        return target


class JobContext(object):
    """JobContexts are used when running an async task to provide easy access
    to the async object.
    """
    def __init__(self, async):
        """Initialize a context with an async task."""
        from .async import Async

        if not isinstance(async, Async):
            raise TypeError("async must be an Async instance.")

        self._async = async
        async.set_job_context(self)

        _init()

    @property
    def async(self):
        """Get a reference to this context's async object."""
        return self._async

    def __enter__(self):
        """Enter the context, add this async to the executing context stack."""
        _local_context._executing_async.append(self._async)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context, pop this async from the executing context stack.
        """
        _local_context._executing_async.pop()
        return False


def _init():
    """Initialize the furious context and registry.

    NOTE: Do not directly run this method.
    """
    global _local_context

    # If there is a context and it is initialized to this request,
    # return, otherwise reinitialize the _local_context.
    if (hasattr(_local_context, '_initialized') and
            _local_context._initialized == os.environ['REQUEST_ID_HASH']):
        return

    _local_context = threading.local()

    # So that we do not inadvertently reinitialize the local context.
    _local_context._initialized = os.environ['REQUEST_ID_HASH']

    # Used to track the context object stack.
    _local_context.registry = []

    # Used to provide easy access to the currently running Async job.
    _local_context._executing_async_context = None
    _local_context._executing_async = []

    return _local_context


def _get_local_context():
    """Return a reference to the current _local_context.

    NOTE: This function is not for general usage, it is meant for
    special uses, like unit tests.
    """
    return _local_context
