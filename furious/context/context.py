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
from ..job_utils import path_to_reference
from ..job_utils import reference_to_path

from .. import errors

DEFAULT_TASK_BATCH_SIZE = 100


class Context(object):
    """Furious context object.

    NOTE: Use the module's new function to get a context, do not manually
    instantiate.
    """
    def __init__(self, **options):
        self._tasks = []
        self._task_ids = []
        self._tasks_inserted = False
        self._insert_success_count = 0
        self._insert_failed_count = 0

        id = options.get('id')
        if not id:
            id = uuid.uuid4().hex
        self._id = id

        self._persistence_engine = options.get('persistence_engine', None)
        if self._persistence_engine:
            options['persistence_engine'] = reference_to_path(
                self._persistence_engine)

        self._options = options

        self._insert_tasks = options.pop('insert_tasks', _insert_tasks)
        if not callable(self._insert_tasks):
            raise TypeError('You must provide a valid insert_tasks function.')

    @property
    def id(self):
        return self._id

    @property
    def insert_success(self):
        return self._insert_success_count

    @property
    def insert_failed(self):
        return self._insert_failed_count

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type and self._tasks:
            self._handle_tasks()

        return False

    def _handle_tasks_insert(self, batch_size=None):
        """Convert all Async's into tasks, then insert them into queues."""
        if self._tasks_inserted:
            raise errors.ContextAlreadyStartedError(
                "This Context has already had its tasks inserted.")

        task_map = self._get_tasks_by_queue()
        for queue, tasks in task_map.iteritems():
            for batch in _task_batcher(tasks, batch_size=batch_size):
                inserted = self._insert_tasks(batch, queue=queue)
                if isinstance(inserted, (int, long)):
                    # Don't blow up on insert_tasks that don't return counts.
                    self._insert_success_count += inserted
                    self._insert_failed_count += len(batch) - inserted

    def _handle_tasks(self):
        """Convert all Async's into tasks, then insert them into queues.
        Also mark all tasks inserted to ensure they are not reinserted later.
        """
        self._handle_tasks_insert()

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

        Takes an Async object or the arguments to construct an Async
        object as arguments.  Returns the newly added Async object.
        """
        from ..async import Async
        from ..batcher import Message

        if self._tasks_inserted:
            raise errors.ContextAlreadyStartedError(
                "This Context has already had its tasks inserted.")

        if not isinstance(target, (Async, Message)):
            target = Async(target, args, kwargs, **options)

        self._tasks.append(target)

        return target

    def start(self):
        """Insert this Context's tasks so they start executing."""
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
            options['insert_tasks'] = reference_to_path(self._insert_tasks)

        if self._persistence_engine:
            options['persistence_engine'] = reference_to_path(
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
    def from_dict(cls, context_options_dict):
        """Return a context job from a dict output by Context.to_dict."""
        import copy

        context_options = copy.deepcopy(context_options_dict)

        tasks_inserted = context_options.pop('_tasks_inserted', False)
        task_ids = context_options.pop('_task_ids', [])

        insert_tasks = context_options.pop('insert_tasks', None)
        if insert_tasks:
            context_options['insert_tasks'] = path_to_reference(insert_tasks)

        # The constructor expects a reference to the persistence engine.
        persistence_engine = context_options.pop('persistence_engine', None)
        if persistence_engine:
            context_options['persistence_engine'] = path_to_reference(
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
    inserted. Return the number of successfully inserted tasks.
    """
    from google.appengine.api import taskqueue

    if not tasks:
        return 0

    try:
        taskqueue.Queue(name=queue).add(tasks, transactional=transactional)
        return len(tasks)
    except (taskqueue.BadTaskStateError,
            taskqueue.TaskAlreadyExistsError,
            taskqueue.TombstonedTaskError,
            taskqueue.TransientError):
        count = len(tasks)
        if count <= 1:
            return 0

        inserted = _insert_tasks(tasks[:count / 2], queue, transactional)
        inserted += _insert_tasks(tasks[count / 2:], queue, transactional)

        return inserted


def _task_batcher(tasks, batch_size=None):
    """Batches large task lists into groups of 100 so that they can all be
    inserted.
    """
    from itertools import izip_longest

    if not batch_size:
        batch_size = DEFAULT_TASK_BATCH_SIZE

    args = [iter(tasks)] * batch_size
    return ([task for task in group if task] for group in izip_longest(*args))

