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
import copy
from functools import partial
from functools import wraps
import json
import os
import time
import uuid

from furious.job_utils import decode_callbacks
from furious.job_utils import encode_callbacks
from furious.job_utils import get_function_path_and_options
from furious.job_utils import path_to_reference
from furious.job_utils import reference_to_path

from furious import errors


__all__ = ['ASYNC_DEFAULT_QUEUE', 'ASYNC_ENDPOINT', 'Async', 'defaults']


ASYNC_DEFAULT_QUEUE = 'default'
ASYNC_ENDPOINT = '/_queue/async'
MAX_DEPTH = 100
MAX_RESTARTS = 10
DISABLE_RECURSION_CHECK = -1
RETRY_SLEEP_SECS = 4

DEFAULT_RETRY_OPTIONS = {
    'task_retry_limit': MAX_RESTARTS
}


class Async(object):
    def __init__(self, target, args=None, kwargs=None, **options):
        self._options = {}

        # Make sure nothing is snuck in.
        _check_options(options)

        self._update_job(target, args, kwargs)

        self.update_options(**options)

        self._initialize_recursion_depth()

        self._context_id = self._get_context_id()
        self._parent_id = self._get_parent_id()
        self._id = self._get_id()

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
            raise errors.AlreadyExecutedError(
                'You can not execute an executed job.')

        if self._executing:
            raise errors.AlreadyExecutingError(
                'Job is already executing, can not set executing.')

        self._executing = executing

    @property
    def result(self):
        if not self.executed:
            raise errors.NotExecutedError(
                'You must execute this Async before getting its result.')

        return self._result

    @result.setter
    def result(self, result):
        if not self._executing:
            raise errors.NotExecutingError(
                'The Async must be executing to set its result.')

        self._result = result
        self._executing = False
        self._executed = True

        if self._options.get('persist_result'):
            self._persist_result()

    def _persist_result(self):
        """Store this Async's result in persistent storage."""
        self._prepare_persistence_engine()

        return self._persistence_engine.store_async_result(
            self.id, self.result)

    def _decorate_job(self):
        """Returns the job function.

        A subclass may override `Async._decorate_job` in order to wrap the
        original target using a decorator function.
        """
        function_path = self.job[0]
        func = path_to_reference(function_path)
        return func

    @property
    def function_path(self):
        return self.job[0]

    @property
    def _function_path(self):
        # DEPRECATED: Hanging around for backwards compatibility.
        return self.function_path

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

        except errors.NotInContextError:
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
            raise errors.AsyncRecursionError('Max recursion depth reached.')

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
            raise errors.AlreadyInContextError

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

        if 'id' in options:
            self._id = options['id']

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
        from google.appengine.api.taskqueue import TaskRetryOptions

        self._increment_recursion_level()
        self.check_recursion_depth()

        url = "%s/%s" % (ASYNC_ENDPOINT, self.function_path)

        kwargs = {
            'url': url,
            'headers': self.get_headers().copy(),
            'payload': json.dumps(self.to_dict())
        }
        kwargs.update(copy.deepcopy(self.get_task_args()))

        # Set task_retry_limit
        retry_options = copy.deepcopy(DEFAULT_RETRY_OPTIONS)
        retry_options.update(kwargs.pop('retry_options', {}))
        kwargs['retry_options'] = TaskRetryOptions(**retry_options)

        return Task(**kwargs)

    def start(self, transactional=False, async=False, rpc=None):
        """Insert the task into the requested queue, 'default' if non given.

        If a TransientError is hit the task will re-insert the task. If a
        TaskAlreadyExistsError or TombstonedTaskError is hit the task will
        silently fail.

        If the async flag is set, then the add will be done asynchronously and
        the return value will be the rpc object; otherwise the return value is
        the task itself. If the rpc kwarg is provided, but we're not in async
        mode, then it is ignored.
        """
        from google.appengine.api import taskqueue

        task = self.to_task()
        queue = taskqueue.Queue(name=self.get_queue())
        retry_transient = self._options.get('retry_transient_errors', True)
        retry_delay = self._options.get('retry_delay', RETRY_SLEEP_SECS)

        add = queue.add
        if async:
            add = partial(queue.add_async, rpc=rpc)

        try:
            ret = add(task, transactional=transactional)
        except taskqueue.TransientError:
            # Always re-raise for transactional insert, or if specified by
            # options.
            if transactional or not retry_transient:
                raise

            time.sleep(retry_delay)

            ret = add(task, transactional=transactional)
        except (taskqueue.TaskAlreadyExistsError,
                taskqueue.TombstonedTaskError):
            return

        # TODO: Return a "result" object.
        return ret

    def __deepcopy__(self, *args):
        """In order to support callbacks being Async objects, we need to
        support being deep copied.
        """
        return self

    def to_dict(self):
        """Return this async job as a dict suitable for json encoding."""
        return encode_async_options(self)

    @classmethod
    def from_dict(cls, async):
        """Return an async job from a dict output by Async.to_dict."""
        async_options = decode_async_options(async)

        target, args, kwargs = async_options.pop('job')

        return cls(target, args, kwargs, **async_options)

    def _prepare_persistence_engine(self):
        """Load the specified persistence engine, or the default if none is
        set.
        """
        if self._persistence_engine:
            return

        persistence_engine = self._options.get('persistence_engine')
        if persistence_engine:
            self._persistence_engine = path_to_reference(persistence_engine)
            return

        from furious.config import get_default_persistence_engine

        self._persistence_engine = get_default_persistence_engine()

    def _get_context_id(self):
        """If this async is in a context set the context id."""

        from furious.context import get_current_context

        context_id = self._options.get('context_id')

        if context_id:
            return context_id

        try:
            context = get_current_context()
        except errors.NotInContextError:
            context = None
            self.update_options(context_id=None)

        if context:
            context_id = context.id
            self.update_options(context_id=context_id)

        return context_id

    def _get_parent_id(self):
        """If this async is in within another async set that async id as the
        parent.
        """
        parent_id = self._options.get('parent_id')
        if parent_id:
            return parent_id

        from furious.context import get_current_async

        try:
            async = get_current_async()
        except errors.NotInContextError:
            async = None

        if async:
            parent_id = ":".join([async.parent_id.split(":")[0], async.id])
        else:
            parent_id = self.request_id

        self.update_options(parent_id=parent_id)

        return parent_id

    def _get_id(self):
        """If this async has no id, generate one."""
        id = self._options.get('id')
        if id:
            return id

        id = uuid.uuid4().hex
        self.update_options(id=id)
        return id

    @property
    def id(self):
        """Return this Async's ID value."""
        return self._id

    @property
    def context_id(self):
        """Return this Async's Context Id if it exists."""
        return self._context_id

    @property
    def parent_id(self):
        """Return this Async's Parent Id if it exists."""
        return self._parent_id

    @property
    def full_id(self):
        """Return the full_id for this Async. Consists of the parent id, id and
        context id.
        """
        full_id = ""

        if self.parent_id:
            full_id = ":".join([self.parent_id, self.id])
        else:
            full_id = self.id

        if self.context_id:
            full_id = "|".join([full_id, self.context_id])

        return full_id

    @property
    def request_id(self):
        return os.environ.get('REQUEST_LOG_ID', uuid.uuid4().hex)

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

    @property
    def context_id(self):
        """Return this Async's Context Id if it exists."""
        if not self._context_id:
            self._context_id = self._get_context_id()
            self.update_options(context_id=self._context_id)

        return self._context_id

    def _get_context_id(self):
        """If this async is in a context set the context id."""

        from furious.context import get_current_context

        context_id = self._options.get('context_id')

        if context_id:
            return context_id

        try:
            context = get_current_context()
        except errors.NotInContextError:
            context = None
            self.update_options(context_id=None)

        if context:
            context_id = context.id
            self.update_options(context_id=context_id)

        return context_id


class AsyncResult(object):

    SUCCESS = 1
    ERROR = 2
    ABORT = 3

    def __init__(self, payload=None, status=None):
        self.payload = payload
        self.status = status

    @property
    def success(self):
        """Return True if the status is a success. This is true if the status
        is not an error state. So abort or success. Abort is considered a
        success as it's something expected by the developer. Errors would
        generally only happen in tasks if something unexpected occurred.
        """
        return self.status != self.ERROR

    def to_dict(self):
        """Return the AsyncResult converted to a dictionary and also to an
        serializable format.
        """
        return {
            'status': self.status,
            'payload': self._payload_to_dict()
        }

    def _payload_to_dict(self):
        """When an error status the payload is holding an AsyncException that
        is converted to a serializable dict.
        """
        if self.status != self.ERROR or not self.payload:
            return self.payload

        import traceback

        return {
            "error": self.payload.error,
            "args": self.payload.args,
            "traceback": traceback.format_exception(*self.payload.traceback)
        }


def async_from_options(options):
    """Deserialize an Async or Async subclass from an options dict."""
    _type = options.pop('_type', 'furious.async.Async')

    _type = path_to_reference(_type)

    return _type.from_dict(options)


def encode_async_options(async):
    """Encode Async options for JSON encoding."""
    options = copy.deepcopy(async._options)

    options['_type'] = reference_to_path(async.__class__)

    # JSON don't like datetimes.
    eta = options.get('task_args', {}).get('eta')
    if eta:
        options['task_args']['eta'] = time.mktime(eta.timetuple())

    callbacks = async._options.get('callbacks')
    if callbacks:
        options['callbacks'] = encode_callbacks(callbacks)

    if '_context_checker' in options:
        _checker = options.pop('_context_checker')
        options['__context_checker'] = reference_to_path(_checker)

    if '_process_results' in options:
        _processor = options.pop('_process_results')
        options['__process_results'] = reference_to_path(_processor)

    return options


def decode_async_options(options):
    """Decode Async options from JSON decoding."""
    async_options = copy.deepcopy(options)

    # JSON don't like datetimes.
    eta = async_options.get('task_args', {}).get('eta')
    if eta:
        from datetime import datetime

        async_options['task_args']['eta'] = datetime.fromtimestamp(eta)

    # If there are callbacks, reconstitute them.
    callbacks = async_options.get('callbacks', {})
    if callbacks:
        async_options['callbacks'] = decode_callbacks(callbacks)

    if '__context_checker' in options:
        _checker = options['__context_checker']
        async_options['_context_checker'] = path_to_reference(_checker)

    if '__process_results' in options:
        _processor = options['__process_results']
        async_options['_process_results'] = path_to_reference(_processor)
    return async_options


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
