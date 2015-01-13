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
import abc
import time
import uuid

from furious.job_utils import decode_callbacks
from furious.job_utils import encode_callbacks
from furious.job_utils import path_to_reference
from furious.job_utils import reference_to_path

from furious import errors

DEFAULT_TASK_BATCH_SIZE = 100
RETRY_SLEEP_SECS = 4


class Context(object):
    """Furious context object.

    NOTE: Use the module's new function to get a context, do not manually
    instantiate.
    """
    def __init__(self, **options):
        self._tasks = []
        self._tasks_inserted = False
        self._insert_success_count = 0
        self._insert_failed_count = 0

        self._persistence_engine = options.get('persistence_engine', None)
        if self._persistence_engine:
            options['persistence_engine'] = reference_to_path(
                self._persistence_engine)

        self._options = options

        if '_task_ids' not in self._options:
            self._options['_task_ids'] = []

        self._id = self._get_id()
        self._result = None

        self._insert_tasks = options.pop('insert_tasks', _insert_tasks)
        if not callable(self._insert_tasks):
            raise TypeError('You must provide a valid insert_tasks function.')

    def _get_id(self):
        """If this async has no id, generate one."""
        id = self._options.get('id')
        if id:
            return id

        id = uuid.uuid4().hex
        self._options['id'] = id
        return id

    @property
    def id(self):
        return self._id

    @property
    def task_ids(self):
        return self._options['_task_ids']

    @property
    def insert_success(self):
        return self._insert_success_count

    @property
    def insert_failed(self):
        return self._insert_failed_count

    @property
    def persist_async_results(self):
        return self._options.get('persist_async_results', False)

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

        # QUESTION: Should the persist happen before or after the task
        # insertion?  I feel like this is something that will alter the
        # behavior of the tasks themselves by adding a callback (check context
        # complete) to each Async's callback stack.

        # If we are able to and there is a reason to persist... persist.
        callbacks = self._options.get('callbacks')
        if self._persistence_engine and callbacks:
            self.persist()

        retry_transient = self._options.get('retry_transient_errors', True)
        retry_delay = self._options.get('retry_delay', RETRY_SLEEP_SECS)

        for queue, tasks in task_map.iteritems():
            for batch in _task_batcher(tasks, batch_size=batch_size):
                inserted = self._insert_tasks(
                    batch, queue=queue, retry_transient_errors=retry_transient,
                    retry_delay=retry_delay
                )
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
        _checker = None

        # Ask the persistence engine for an Async to use for checking if the
        # context is complete.
        if self._persistence_engine:
            _checker = self._persistence_engine.context_completion_checker

        for async in self._tasks:
            queue = async.get_queue()
            if _checker:
                async.update_options(_context_checker=_checker)

            task = async.to_task()
            task_map.setdefault(queue, []).append(task)

        return task_map

    def _prepare_persistence_engine(self):
        """Load the specified persistence engine, or the default if none is
        set.
        """

        from furious.config import get_default_persistence_engine

        if self._persistence_engine:
            return

        persistence_engine = self._options.get('persistence_engine')
        if persistence_engine:
            self._persistence_engine = path_to_reference(persistence_engine)
            return

        self._persistence_engine = get_default_persistence_engine()

    def set_event_handler(self, event, handler):
        """Add an Async to be run on event."""
        # QUESTION: Should we raise an exception if `event` is not in some
        # known event-type list?

        self._prepare_persistence_engine()

        callbacks = self._options.get('callbacks', {})
        callbacks[event] = handler
        self._options['callbacks'] = callbacks

    def exec_event_handler(self, event, transactional=False):
        """Execute the Async set to be run on event."""
        # QUESTION: Should we raise an exception if `event` is not in some
        # known event-type list?

        callbacks = self._options.get('callbacks', {})

        handler = callbacks.get(event)

        if not handler:
            raise Exception('Handler not defined!!!')

        handler.start(transactional=transactional)

    def add(self, target, args=None, kwargs=None, **options):
        """Add an Async job to this context.

        Takes an Async object or the arguments to construct an Async
        object as arguments.  Returns the newly added Async object.
        """
        from furious.async import Async
        from furious.batcher import Message

        if self._tasks_inserted:
            raise errors.ContextAlreadyStartedError(
                "This Context has already had its tasks inserted.")

        if not isinstance(target, (Async, Message)):
            target = Async(target, args, kwargs, **options)

        target.update_options(_context_id=self.id)

        if self.persist_async_results:
            target.update_options(persist_result=True)

        self._tasks.append(target)
        self._options['_task_ids'].append(target.id)

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

        return self._persistence_engine.store_context(self)

    @classmethod
    def load(cls, context_id, persistence_engine=None):
        """Load and instantiate a Context from the persistence_engine."""
        if not persistence_engine:
            from furious.config import get_default_persistence_engine
            persistence_engine = get_default_persistence_engine()

        if not persistence_engine:
            raise RuntimeError(
                'Specify a valid persistence_engine to load the context.')

        return persistence_engine.load_context(context_id)

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

        return context

    @property
    def result(self):
        """Return the context result object pulled from the persistence_engine
        if it has been set.
        """
        if not self._result:
            if not self._persistence_engine:
                return None

            self._result = self._persistence_engine.get_context_result(self)

        return self._result


class ContextResultBase(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def items(self):
        """Yield the async reuslts for the context."""
        return

    @abc.abstractmethod
    def has_errors(self):
        """Return the error flag from the completion engine."""
        return

    @abc.abstractmethod
    def values(self):
        """Return the async reuslt values for the context."""
        return


def _insert_tasks(tasks, queue, transactional=False,
                  retry_transient_errors=True,
                  retry_delay=RETRY_SLEEP_SECS):
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
            taskqueue.TombstonedTaskError):
        if len(tasks) <= 1:
            # Task has already been inserted, no reason to report an error here.
            return 0

        # If a list of more than one Tasks is given, a raised exception does
        # not guarantee that no tasks were added to the queue (unless
        # transactional is set to True). To determine which tasks were
        # successfully added when an exception is raised, check the
        # Task.was_enqueued property.
        reinsert = _tasks_to_reinsert(tasks, transactional)
        count = len(reinsert)
        inserted = len(tasks) - count
        inserted += _insert_tasks(reinsert[:count / 2], queue, transactional,
                                  retry_transient_errors, retry_delay)
        inserted += _insert_tasks(reinsert[count / 2:], queue, transactional,
                                  retry_transient_errors, retry_delay)

        return inserted
    except taskqueue.TransientError:
        # Always re-raise for transactional insert, or if specified by
        # options.
        if transactional or not retry_transient_errors:
            raise

        reinsert = _tasks_to_reinsert(tasks, transactional)

        # Retry with a delay, and then let any errors re-raise.
        time.sleep(retry_delay)

        taskqueue.Queue(name=queue).add(reinsert, transactional=transactional)
        return len(tasks)


def _tasks_to_reinsert(tasks, transactional):
    """Return a list containing the tasks that should be reinserted based on the
    was_enqueued property and whether the insert is transactional or not.
    """
    if transactional:
        return tasks

    return [task for task in tasks if not task.was_enqueued]


def _task_batcher(tasks, batch_size=None):
    """Batches large task lists into groups of 100 so that they can all be
    inserted.
    """
    from itertools import izip_longest

    if not batch_size:
        batch_size = DEFAULT_TASK_BATCH_SIZE

    # Ensure the batch size is under the task api limit.
    batch_size = min(batch_size, 100)

    args = [iter(tasks)] * batch_size
    return ([task for task in group if task] for group in izip_longest(*args))

