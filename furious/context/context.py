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

import uuid

from ..job_utils import decode_callbacks
from ..job_utils import encode_callbacks
from ..job_utils import function_path_to_reference
from ..job_utils import get_function_path_and_options


class ContextAlreadyStartedError(Exception):
    """Attempt to set context on an Async that is already executing in a
    context.
    """


class Context(object):
    """Furious context object.

    NOTE: Use the module's new function to get a context, do not manually
    instantiate.
    """
    def __init__(self, **options):
        self._tasks = []

        id = options.get('id')
        if not id:
            id = uuid.uuid4().hex
        self._tasks_inserted = False
        self._id = id

        self._options = options

        self._insert_tasks = options.pop('insert_tasks', _insert_tasks)
        if not callable(self._insert_tasks):
            raise TypeError('You must provide a valid insert_tasks function.')

        self._persistence_engine = options.pop('persistence_engine', None)

    @property
    def id(self):
        return self._id

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type and self._tasks:
            self._handle_tasks()

        return False

    def _handle_tasks(self):
        """Convert all Async's into tasks, then insert them into queues."""
        if self._tasks_inserted:
            raise ContextAlreadyStartedError(
                "This Context has already had its tasks inserted.")

        task_map = self._get_tasks_by_queue()
        for queue, tasks in task_map.iteritems():
            self._insert_tasks(tasks, queue=queue)

        self._tasks_inserted = True

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

    def persist(self):
        """Store the context."""
        if not self._persistence_engine:
            raise RuntimeError(
                'Specify a valid persistence_engine to persist this context.')

        return self._persistence_engine.store_context(self.id, self.to_dict())

    @classmethod
    def load(cls, context_id, persistence_engine):
        """Load and instantiate a Context from the persistence_engine."""
        if not persistence_engine:
            raise RuntimeError(
                'Specify a valid persistence_engine to load the context.')

        return cls.from_dict(persistence_engine.load_context(context_id))

    def to_dict(self):
        """Return this Context as a dict suitable for json encoding."""
        import copy

        options = copy.deepcopy(self._options)

        if self._insert_tasks:
            options['insert_tasks'], _ = get_function_path_and_options(
                self._insert_tasks)

        if self._persistence_engine:
            options['persistence_engine'], _ = get_function_path_and_options(
                self._persistence_engine)

        options.update({
            '_tasks_inserted': self._tasks_inserted,
            '_task_ids': [async.id for async in self._tasks]
        })

        callbacks = self._options.get('callbacks')
        if callbacks:
            options['callbacks'] = encode_callbacks(callbacks)

        return options

    @classmethod
    def from_dict(cls, context):
        """Return a context job from a dict output by Context.to_dict."""
        import copy

        context_options = copy.deepcopy(context)

        tasks_inserted = context_options.pop('_tasks_inserted', False)
        task_ids = context_options.pop('_task_ids', [])

        insert_tasks = context_options.pop('insert_tasks', None)
        if insert_tasks:
            context_options['insert_tasks'] = function_path_to_reference(
                insert_tasks)

        persistence_engine = context_options.pop('persistence_engine', None)
        if persistence_engine:
            context_options['persistence_engine'] = function_path_to_reference(
                persistence_engine)

        # If there are callbacks, reconstitute them.
        callbacks = context_options.pop('callbacks', None)
        if callbacks:
            context_options['callbacks'] = decode_callbacks(callbacks)

        context = cls(**context_options)

        context._tasks_inserted = tasks_inserted
        context._task_ids = task_ids

        return context


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

