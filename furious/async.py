"""
Core async task wrapper.  This module contains the Async class, which is
used to create asynchronous jobs.  To use,

    # Create a task.
    work = Async(
        job=("function.to.run", args, kwargs),
        task_args={"task": "kwargs"},
        queue="yourqueue"
    )

    # Enqueue the task.
    work.start()

*or*, the equivalent more verbose form:

    # Create a task.
    work = Async()
    work.set_job("function.to.run", *args, **kwargs)
    work.update_options(task_args={"task": "kwargs"}, queue="yourqueue")

    # Enqueue the task.
    work.start()

Job may be one of the following supported formats:

    "path_to_function_with_no_args"
    ("path_to_function_with_no_args")
    ("path_to_function", args, kwargs)
    ("path_to_function", args)
    ("path_to_function", kwargs)

    reference_to_function_with_no_args
    (reference_to_function_with_no_args)
    (reference_to_function, args, kwargs)
    (reference_to_function, args)
    (reference_to_function, kwargs)

"""

try:
    import json
except ImportError:
    import simplejson as json


from .job_utils import check_job


ASYNC_DEFAULT_QUEUE = 'default'
ASYNC_ENDPOINT = '/_ah/queue/async'


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

