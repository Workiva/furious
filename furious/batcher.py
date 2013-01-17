import json

MESSAGE_DEFAULT_QUEUE = 'default'
METHOD_TYPE = 'PULL'


class Message(object):

    def __init__(self, **options):
        self._options = {}

        self.update_options(**options)

    def get_options(self):
        """Return this message's configuration options."""
        return self._options

    def update_options(self, **options):
        """Safely update this message's configuration options."""
        self._options.update(options)

    def get_queue(self):
        """Return the queue the task should run in."""
        return self._options.get('queue', MESSAGE_DEFAULT_QUEUE)

    def get_task_args(self):
        """Get user-specified task kwargs."""
        return self._options.get('task_args', {})

    def to_task(self):
        """Return a task object representing this message."""
        from google.appengine.api.taskqueue import Task

        task_args = self.get_task_args().copy()

        payload = None
        if 'payload' in task_args:
            payload = task_args.pop('payload')

        kwargs = {
            'method': METHOD_TYPE,
            'payload': json.dumps(payload)
        }

        kwargs.update(task_args)

        return Task(**kwargs)

    def insert(self):
        """Insert the pull task into the requested queue, 'default' if non
        given.
        """
        from google.appengine.api.taskqueue import Queue

        task = self.to_task()
        Queue(name=self.get_queue()).add(task)

    def to_dict(self):
        """Return this message as a dict suitable for json encoding."""
        import copy

        options = copy.deepcopy(self._options)

        # JSON don't like datetimes.
        eta = options.get('task_args', {}).get('eta')
        if eta:
            import time

            options['task_args']['eta'] = time.mktime(eta.timetuple())

        return options

    @classmethod
    def from_dict(cls, message):
        """Return an message from a dict output by Async.to_dict."""

        message_options = message.copy()

        # JSON don't like datetimes.
        eta = message_options.get('task_args', {}).get('eta')
        if eta:
            from datetime import datetime

            message_options['task_args']['eta'] = datetime.fromtimestamp(eta)

        return Message(**message_options)


def insert_messsage_processor():
    pass


def fetch_messages():
    pass


def bump_batch():
    pass
