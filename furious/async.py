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

try:
    import json
except ImportError:
    import simplejson as json

from functools import wraps

import sys

from .job_utils import check_job


__all__ = ['ASYNC_DEFAULT_QUEUE', 'ASYNC_ENDPOINT', 'Async', 'defaults']


ASYNC_DEFAULT_QUEUE = 'default'
ASYNC_ENDPOINT = '/_ah/queue/async'


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


class Async(object):
    def __init__(self, job=None, **kwargs):
        self._options = {}
        self._function_path = None

        if job:
            job = check_job(job)
            self._options['job'] = job
            self._function_path = job[0]

        self.update_options(**kwargs)

    def set_job(self, function, *args, **kwargs):
        """Specify the function this async job is to execute when run."""
        job = check_job((function, args, kwargs))
        self._options['job'] = job
        self._function_path = job[0]

    def get_options(self):
        """Return this async job's configuration options."""
        return self._options

    def update_options(self, **options):
        """Safely update this async job's configuration options."""
        # TODO: What logic needs enforced here?
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

    def to_dict(self):
        """Return this async job as a dict suitable for json encoding."""
        import copy

        options = copy.deepcopy(self._options)

        # JSON don't like datetimes.
        eta = options.get('task_args', {}).get('eta')
        if eta:
            import datetime
            import time

            options['task_args']['eta'] = time.mktime((
                datetime.datetime.now() + datetime.timedelta(30)).timetuple())

        return options

    @classmethod
    def from_dict(cls, async):
        """Return an async job from a dict output by Async.to_dict."""

        # JSON don't like datetimes.
        eta = async.get('task_args', {}).get('eta')
        if eta:
            import datetime

            async['task_args']['eta'] = datetime.datetime.fromtimestamp(eta)

        return Async(**async)


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

    function = _get_function_reference(function_path)

    return function(*args, **kwargs)


def _get_function_reference(function_path):
    """Convert a function path reference to a reference."""
    if '.' not in function_path:
        try:
            return globals()["__builtins__"][function_path]
        except KeyError:
            try:
                return getattr(globals()["__builtins__"], function_path)
            except AttributeError:
                pass

        try:
            return globals()[function_path]
        except KeyError:
            pass

        raise Exception('Unable to find function "%s".' % (function_path,))

    module_path, function_name = function_path.rsplit('.', 1)

    if module_path in sys.modules:
        module = sys.modules[module_path]
    else:
        module = __import__(name=module_path, fromlist=[function_name])

    try:
        return getattr(module, function_name)
    except AttributeError:
        raise Exception('Unable to find function "%s".' % (function_path,))

