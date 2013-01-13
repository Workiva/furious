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

NOTE: It is generally preferable to use the higher level helper method
found in this package to instantiate contexts.


Usage:


    with Context() as current_context:
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
import logging


class ContextAlreadyStartedError(Exception):
    """Attempt to set context on an Async that is already executing in a
    context.
    """


class Context(object):
    """Furious context object.

    NOTE: Use the module's new function to get a context, do not manually
    instantiate.
    """
    def __init__(self, insert_tasks=None):
        self._tasks = []

        self.insert_tasks = insert_tasks or _insert_tasks

        self._tasks_inserted = False

        self._persistence_id = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type and self._tasks:
            self._handle_tasks()

        return False

    def _handle_tasks(self):
        """Convert all Async's into tasks, then insert them into queues."""
        from furious.extras.appengine.ndb import build_async_tree
        from furious.extras.appengine.ndb import MarkerTree
        if self._tasks_inserted:
            raise ContextAlreadyStartedError(
                "This Context has already had its tasks inserted.")

        logging.info("start and make async tree")
        marker = build_async_tree(self._tasks)
        markerTree = MarkerTree(tree=marker.to_dict())
        mt_future = markerTree.put_async()
        marker_persist = marker.persist()
        self._persistence_id = marker_persist.key.id()
        logging.info("self._persistence_id = %s"%self._persistence_id)
        logging.info("persistence_key = %s"%marker_persist.key)

        task_map = self._get_tasks_by_queue()
        for queue, tasks in task_map.iteritems():
            self.insert_tasks(tasks, queue=queue)

        self._tasks_inserted = True
        mt_future.wait()

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

        Takes an Async object or the arguments to construct an Async
        object as arguments.  Returns the newly added Async object.
        """
        from ..async import Async

        if self._tasks_inserted:
            raise ContextAlreadyStartedError(
                "This Context has already had its tasks inserted.")

        if not isinstance(target, Async):
            target = Async(target, args, kwargs, **options)

        self._tasks.append(target)

        return target

    def start(self):
        """Insert this Context's tasks executing."""

        if self._tasks:
            self._handle_tasks()


def _insert_tasks(tasks, queue, transactional=False):
    """Insert a batch of tasks into the specified queue. If an error occurs
    during insertion, split the batch and retry until they are successfully
    inserted.
    """
    from google.appengine.api import taskqueue

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

