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

"""Core async task wrapper.  This module contains the `Async` class, which is
used to create asynchronous jobs, and a `defaults` decorator you may use to
specify default settings for a particular async task.  To use,

    # Create a task.
    work = Async(
        target="function.to.run",
        args=(args, for, function),
        kwargs={'keyword': arguments, 'to': target},
        task_args={"appengine": 1, "task": "kwargs"},
        queue="yourqueue"
    )

    # Enqueue the task.
    work.start()

*or*, set default arguments for a function:

    @defaults(task_args={"appengine": 1, "task": "kwargs"}, queue="yourqueue")
    def run_me(*args, **kwargs):
        pass

    # Create a task.
    work = Async(
        target=run_me,
        args=(args, for, function),
        kwargs={'keyword': arguments, 'to': target},
    )

    # Enqueue the task.
    work.start()

You may also update options after instantiation:

    # Create a task.
    work = Async(
        target="function.to.run",
        args=(args, for, function),
        kwargs={'keyword': arguments, 'to': target}
    )

    work.update_options(task_args={"appengine":1, "task": "kwargs"},
                        queue="yourqueue")

    # Enqueue the task.
    work.start()

The order of precedence is:
    1) options specified when calling start.
    2) options specified using update_options.
    3) options specified in the constructor.
    4) options specified by @defaults decorator.
"""

from functools import wraps

import json

from .job_utils import decode_callbacks
from .job_utils import encode_callbacks
from .job_utils import get_function_path_and_options
from .job_utils import reference_to_path


__all__ = ['ASYNC_DEFAULT_QUEUE', 'ASYNC_ENDPOINT', 'Async', 'defaults']


ASYNC_DEFAULT_QUEUE = 'default'
ASYNC_ENDPOINT = '/_ah/queue/async'
MAX_DEPTH = 100
DISABLE_RECURSION_CHECK = -1


class NotExecutedError(Exception):
    """This Async has not yet been executed."""


class NotExecutingError(Exception):
    """This Async in not currently executing."""


class AlreadyExecutedError(Exception):
    """This Async has already been executed."""


class AlreadyExecutingError(Exception):
    """This Async is currently executing."""


class Abort(Exception):
    """This Async needs to be aborted immediately. Only an info level logging
    message will be output about the aborted job.
    """


class AbortAndRestart(Exception):
    """This Async needs to be aborted immediately and restarted."""


class AsyncRecursionError(Abort):
    """This Async has hit the max recursion depth, it should be aborted."""


class Async(object):
    def __init__(self, target, args=None, kwargs=None, **options):
        self._options = {}

        # Make sure nothing is snuck in.
        _check_options(options)

        self._update_job(target, args, kwargs)

        self.update_options(**options)

        self._initialize_recursion_depth()

        self._execution_context = None

        self._executing = False
        self._executed = False

        self._persistence_engine = None

        self._result = None

    @property
    def executed(self):
        return self._executed

    @property
    def executing(self):
        return self._executing

    @executing.setter
    def executing(self, executing):
        if self._executed:
            raise AlreadyExecutedError(
                'You can not execute an executed job.')

        if self._executing:
            raise AlreadyExecutingError(
                'Job is already executing, can not set executing.')

        self._executing = executing

    @property
    def result(self):
        if not self.executed:
            raise NotExecutedError(
                'You must execute this Async before getting its result.')

        return self._result

    @result.setter
    def result(self, result):
        if not self._executing:
            raise NotExecutingError(
                'The Async must be executing to set its result.')

        self._result = result
        self._executing = False
        self._executed = True

    @property
    def _function_path(self):
        return self.job[0]

    @property
    def job(self):
        """job is stored as a (function path, args, kwargs) tuple."""
        return self._options['job']

    @property
    def recursion_depth(self):
        """Get the current recursion depth.  `None` indicates uninitialized
        recursion info.
        """
        recursion_options = self._options.get('_recursion', {})
        return recursion_options.get('current', None)

    def _initialize_recursion_depth(self):
        """Ensure recursion info is initialized, if not, initialize it."""
        from furious.context import NotInContextError
        from furious.context import get_current_async

        recursion_options = self._options.get('_recursion', {})

        current_depth = recursion_options.get('current', 0)
        max_depth = recursion_options.get('max', MAX_DEPTH)

        try:
            executing_async = get_current_async()

            # If this async is within an executing async, use the depth off
            # that async.  Otherwise use the depth set in the async's options.
            current_depth = executing_async.recursion_depth

            # If max_depth does not equal MAX_DEPTH, it is custom. Otherwise
            # use the max_depth from the containing async.
            if max_depth == MAX_DEPTH:
                executing_options = executing_async.get_options().get(
                    '_recursion', {})
                max_depth = executing_options.get('max', max_depth)

        except NotInContextError:
            # This Async is not being constructed inside an executing Async.
            pass

        # Store the recursion info.
        self.update_options(_recursion={'current': current_depth,
                                        'max': max_depth})

    def check_recursion_depth(self):
        """Check recursion depth, raise AsyncRecursionError if too deep."""
        from furious.async import MAX_DEPTH

        recursion_options = self._options.get('_recursion', {})
        max_depth = recursion_options.get('max', MAX_DEPTH)

        # Check if recursion check has been disabled, then check depth.
        if (max_depth != DISABLE_RECURSION_CHECK and
                self.recursion_depth > max_depth):
            raise AsyncRecursionError('Max recursion depth reached.')

    def _update_job(self, target, args, kwargs):
        """Specify the function this async job is to execute when run."""
        target_path, options = get_function_path_and_options(target)

        assert isinstance(args, (tuple, list)) or args is None
        assert isinstance(kwargs, dict) or kwargs is None

        if options:
            self.update_options(**options)

        self._options['job'] = (target_path, args, kwargs)

    def set_execution_context(self, execution_context):
        """Set the ExecutionContext this async is executing under."""
        if self._execution_context:
            from .context import AlreadyInContextError
            raise AlreadyInContextError

        self._execution_context = execution_context

    def get_options(self):
        """Return this async job's configuration options."""
        return self._options

    def update_options(self, **options):
        """Safely update this async job's configuration options."""

        _check_options(options)

        if 'persistence_engine' in options:
            options['persistence_engine'] = reference_to_path(
                options['persistence_engine'])

        self._options.update(options)

    def get_callbacks(self):
        """Return this async job's callback map."""
        return self._options.get('callbacks', {})

    def get_headers(self):
        """Create and return task headers."""
        # TODO: Encode some options into a header here.
        return self._options.get('headers', {})

    def get_queue(self):
        """Return the queue the task should run in."""
        return self._options.get('queue', ASYNC_DEFAULT_QUEUE)

    def get_task_args(self):
        """Get user-specified task kwargs."""
        return self._options.get('task_args', {})

    def to_task(self):
        """Return a task object representing this async job."""
        from google.appengine.api.taskqueue import Task

        self._increment_recursion_level()
        self.check_recursion_depth()

        url = "%s/%s" % (ASYNC_ENDPOINT, self._function_path)

        kwargs = {
            'url': url,
            'headers': self.get_headers().copy(),
            'payload': json.dumps(self.to_dict()),
        }
        kwargs.update(self.get_task_args())

        return Task(**kwargs)

    def start(self):
        """Insert the task into the requested queue, 'default' if non given.

        If a TransientError is hit the task will re-insert the task.

        If a TaskAlreadyExistsError or TombstonedTaskError is hit the task will
        silently fail.
        """
        from google.appengine.api import taskqueue

        task = self.to_task()

        try:
            taskqueue.Queue(name=self.get_queue()).add(task)

        except taskqueue.TransientError:
            taskqueue.Queue(name=self.get_queue()).add(task)

        except (taskqueue.TaskAlreadyExistsError,
                taskqueue.TombstonedTaskError):
            return

        # TODO: Return a "result" object.

    def __deepcopy__(self, *args):
        """In order to support callbacks being Async objects, we need to
        support being deep copied.
        """
        return self

    def to_dict(self):
        """Return this async job as a dict suitable for json encoding."""
        import copy

        options = copy.deepcopy(self._options)

        # JSON don't like datetimes.
        eta = options.get('task_args', {}).get('eta')
        if eta:
            import time

            options['task_args']['eta'] = time.mktime(eta.timetuple())

        callbacks = self._options.get('callbacks')
        if callbacks:
            options['callbacks'] = encode_callbacks(callbacks)

        return options

    @classmethod
    def from_dict(cls, async):
        """Return an async job from a dict output by Async.to_dict."""
        import copy

        async_options = copy.deepcopy(async)

        # JSON don't like datetimes.
        eta = async_options.get('task_args', {}).get('eta')
        if eta:
            from datetime import datetime

            async_options['task_args']['eta'] = datetime.fromtimestamp(eta)

        target, args, kwargs = async_options.pop('job')

        # If there are callbacks, reconstitute them.
        callbacks = async_options.get('callbacks', {})
        if callbacks:
            async_options['callbacks'] = decode_callbacks(callbacks)

        return cls(target, args, kwargs, **async_options)

    def _restart(self):
        """Restarts the executing Async.

        If the Async is executing, then it will reset the _executing flag, and
        restart this job. This means that the job will not necessarily execute
        immediately, or on the same machine, as it goes back into the queue.
        """

        if not self._executing:
            raise NotExecutingError("Must be executing to restart the job, "
                                    "perhaps you want Async.start()")

        self._executing = False

        return self.start()

    def _prepare_persistence_engine(self):
        """Load the specified persistence engine, or the default if none is
        set.
        """
        if self._persistence_engine:
            return

        persistence_engine = self._options.get('persistence_engine')
        if persistence_engine:
            from .job_utils import path_to_reference
            self._persistence_engine = path_to_reference(persistence_engine)
            return

        from .config import get_default_persistence_engine

        self._persistence_engine = get_default_persistence_engine()

    def persist_result(self):
        """Store this Async's result in persistent storage."""
        self._prepare_persistence_engine()

        return self._persistence_engine.store_async_result(self)

    def persist_marker(self):
        """Store a marker to indicate this Async's completion status."""
        self._prepare_persistence_engine()

        return self._persistence_engine.store_async_marker(self)

    def _increment_recursion_level(self):
        """Increment current_depth based on either defaults or the enclosing
        Async.
        """
        # Update the recursion info.  This is done so that if an async created
        # outside an executing context, or one previously created is later
        # loaded from storage, that the "current" setting is correctly set.
        self._initialize_recursion_depth()

        recursion_options = self._options.get('_recursion', {})
        current_depth = recursion_options.get('current', 0) + 1
        max_depth = recursion_options.get('max', MAX_DEPTH)

        # Increment and store
        self.update_options(_recursion={'current': current_depth,
                                        'max': max_depth})


def defaults(**options):
    """Set default Async options on the function decorated.

    Note: you must pass the decorated function by reference, not as a
    "path.string.to.function" for this to have any effect.
    """
    _check_options(options)

    def real_decorator(function):
        function._async_options = options

        @wraps(function)
        def wrapper(*args, **kwargs):
            return function(*args, **kwargs)
        return wrapper

    return real_decorator


def _check_options(options):
    """Make sure no one passes something not allowed in."""
    if not options:
        return

    assert 'job' not in options
    #assert 'callbacks' not in options

