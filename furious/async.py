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


__all__ = ['ASYNC_DEFAULT_QUEUE', 'ASYNC_ENDPOINT', 'Async', 'defaults']


ASYNC_DEFAULT_QUEUE = 'default'
ASYNC_ENDPOINT = '/_ah/queue/async'
MAX_DEPTH = 100


class NotExecutedError(Exception):
    """This Async has not yet been executed."""


class NotExecutingError(Exception):
    """This Async in not currently executing."""


class AlreadyExecutedError(Exception):
    """This Async has already been executed."""


class AlreadyExecutingError(Exception):
    """This Async is currently executing."""


class AbortAndRestart(Exception):
    """This Async needs to be aborted immediately and restarted."""


class Async(object):
    def __init__(self, target, args=None, kwargs=None, **options):
        self._options = {}

        # Make sure nothing is snuck in.
        _check_options(options)

        self._update_job(target, args, kwargs)

        self.update_options(**options)

        self._execution_context = None

        self._executing = False
        self._executed = False

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
        return self._options['job'][0]

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

        if self._check_and_update_depth():
            import logging

            logging.warning('Async %s execution has reached max_depth and is '
                            'ceasing execution.' % self._function_path)
            return

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

    def _check_and_update_depth(self):
        """Check to see if we've exceeded the max recursion depth.

        Returns True if current depth > max depth.

        When that is False, it updates this Async with the incremented depth
        and the max depth.
        """

        from .context._local import get_local_context

        recursion_options = self.get_options().get('_recursion', {})
        current_depth = recursion_options.get('current', 0)
        max_depth = recursion_options.get('max', MAX_DEPTH)

        execution_context = get_local_context()._executing_async_context

        if execution_context:
            # We're in an existing Async chain

            wrapping_options = execution_context.async.get_options()
            wrapping_recursion = wrapping_options.get('_recursion', {})
            current_depth = wrapping_recursion.get('current', current_depth)
            max_depth = wrapping_recursion.get('max', max_depth)

        if current_depth > max_depth:
            return True

        # Increment and store
        self.update_options(_recursion={'current': current_depth + 1,
                                        'max': max_depth})

        return False


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

