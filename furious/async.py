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

from .job_utils import get_function_path_and_options
from .job_utils import function_path_to_reference


__all__ = ['ASYNC_DEFAULT_QUEUE', 'ASYNC_ENDPOINT', 'Async', 'defaults']


ASYNC_DEFAULT_QUEUE = 'default'
ASYNC_ENDPOINT = '/_ah/queue/async'


class Async(object):
    def __init__(self, target, args=None, kwargs=None, **options):
        self._options = {}

        # Make sure nothing is snuck in.
        _check_options(options)

        self._update_job(target, args, kwargs)

        self.update_options(**options)

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

    def get_options(self):
        """Return this async job's configuration options."""
        return self._options

    def update_options(self, **options):
        """Safely update this async job's configuration options."""

        _check_options(options)

        self._options.update(options)

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
        """Insert the task into the requested queue, 'default' if non given."""
        from google.appengine.api.taskqueue import Queue

        task = self.to_task()
        Queue(name=self.get_queue()).add(task)
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

        return options

    @classmethod
    def from_dict(cls, async):
        """Return an async job from a dict output by Async.to_dict."""

        async_options = async.copy()

        # JSON don't like datetimes.
        eta = async_options.get('task_args', {}).get('eta')
        if eta:
            from datetime import datetime

            async_options['task_args']['eta'] = datetime.fromtimestamp(eta)

        target, args, kwargs = async_options.pop('job')

        return Async(target, args, kwargs, **async_options)


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


def run_job(async):
    """Takes an async object and executes its job."""
    async_options = async.get_options()

    job = async_options.get('job')
    if not job:
        raise Exception('This async contains no job to execute!')

    function_path, args, kwargs = job

    if args is None:
        args = ()

    if kwargs is None:
        kwargs = {}

    function = function_path_to_reference(function_path)

    return function(*args, **kwargs)

