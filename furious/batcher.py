import json
import time

from google.appengine.api import memcache

from .async import Async

MESSAGE_DEFAULT_QUEUE = 'default_pull'
MESSAGE_PROCESSOR_NAME = 'processor'
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


class MessageProcessor(Async):
    """Async message processor for processing messages in the pull queue."""

    def __init__(self, target, args=None, kwargs=None, tag=None, freq=30,
                 **options):
        super(MessageProcessor, self).__init__(target, args, kwargs, **options)

        self.frequency = freq
        self.tag = tag if tag else MESSAGE_PROCESSOR_NAME

    def to_task(self):
        """Return a task object representing this MessageProcessor job."""
        task_args = self.get_task_args()

        # check for name in task args
        name = task_args.get('name', MESSAGE_PROCESSOR_NAME)

        # if the countdown isn't in the task_args set it to the frequency
        if not 'countdown' in task_args:
            task_args['countdown'] = self.frequency

        task_args['name'] = "%s-%s-%s-%s" % (
            name, self.tag, self.current_batch, self.time_throttle)

        self.update_options(task_args=task_args)

        return super(MessageProcessor, self).to_task()

    @property
    def group_key(self):
        """Return the :class: `str` group key based off of the tag."""
        return 'agg-batch-%s' % (self.tag)

    @property
    def current_batch(self):
        """Return the batch id for the tag.

        :return: :class: `int` current batch id
        """
        current_batch = memcache.get(self.group_key)

        if not current_batch:
            memcache.add(self.group_key, 1)
            current_batch = 1

        return current_batch

    @property
    def time_throttle(self):
        """Return an :class: `int` of the currrent time divided by the
        processor frequency. Frequency cannot be less than one and will default
        to one if it is.

        :return: :class: `int`
        """
        return int(time.time() / max(1, self.frequency))


def fetch_messages():
    pass


def bump_batch():
    pass
