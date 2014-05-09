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

import json
import logging
import time
import uuid

from google.appengine.api import memcache
from google.appengine.runtime.apiproxy_errors import DeadlineExceededError

from furious.async import Async

MESSAGE_DEFAULT_QUEUE = 'default-pull'
MESSAGE_PROCESSOR_NAME = 'processor'
MESSAGE_BATCH_NAME = 'agg-batch'
METHOD_TYPE = 'PULL'


class Message(object):

    def __init__(self, **options):
        self._options = {}

        self.update_options(**options)

        self._id = self._get_id()

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

    def _get_id(self):
        """If this message has no id, generate one."""
        id = self._options.get('id')
        if id:
            return id

        id = uuid.uuid4().hex
        self.update_options(id=id)
        return id

    @property
    def id(self):
        """Return this Message's ID value."""
        return self._id

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
        return "%s-%s" % (MESSAGE_BATCH_NAME, self.tag)

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


class MessageIterator(object):
    """This iterator will return a batch of messages for a given group.

    This iterator should be directly used when trying to avoid the lease
    operation inside a transaction, or when other flows are needed.
    """

    def __init__(self, tag, queue_name, size, duration=60, deadline=10,
                 auto_delete=True):
        """The generator will yield json deserialized payloads from tasks with
        the corresponding tag.

        :param tag: :class: `str` Pull queue tag to query against
        :param queue_name: :class: `str` Name of PULL queue holding tasks to
                           lease.
        :param size: :class: `int` The number of items to pull at once
        :param duration: :class: `int` After this time, the tasks may be leased
                         again. Tracked in seconds
        :param deadline: :class: `int` The time in seconds to wait for the rpc.
        :param auto_delete: :class: `bool` Delete tasks when iteration is
                            complete.

        :return: :class: `iterator` of json deserialized payloads
        """
        from google.appengine.api.taskqueue import Queue

        self.queue_name = queue_name
        self.queue = Queue(name=self.queue_name)

        self.tag = tag
        self.size = size
        self.duration = duration
        self.auto_delete = auto_delete
        self.deadline = deadline

        self._messages = []
        self._processed_messages = []
        self._fetched = False

    def fetch_messages(self):
        """Fetch messages from the specified pull-queue.

        This should only be called a single time by a given MessageIterator
        object.  If the MessageIterator is iterated over again, it should
        return the originally leased messages.
        """
        if self._fetched:
            return

        start = time.time()

        loaded_messages = self.queue.lease_tasks_by_tag(
            self.duration, self.size, tag=self.tag, deadline=self.deadline)

        # If we are within 0.1 sec of our deadline and no messages were
        # returned, then we are hitting queue contention issues and this
        # should be a DeadlineExceederError.
        # TODO: investigate other ways around this, perhaps async leases, etc.
        if (not loaded_messages and
                round(time.time() - start, 1) >= self.deadline - 0.1):
            raise DeadlineExceededError()

        self._messages.extend(loaded_messages)

        self._fetched = True

        logging.debug("Calling fetch messages with %s:%s:%s:%s:%s:%s" % (
            len(self._messages), len(loaded_messages),
            len(self._processed_messages), self.duration, self.size, self.tag))

    def __iter__(self):
        """Initialize this MessageIterator for iteration.

        If messages have not been fetched, fetch them.  If messages have been
        fetched, reset self._messages and self._processed_messages for
        re-iteration.  The reset is done to prevent deleting messages that were
        never applied.
        """
        if self._processed_messages:
            # If the iterator is used within a transaction, and there is a
            # retry we need to re-process the original messages, not new
            # messages.
            self._messages = list(
                set(self._messages) | set(self._processed_messages))
            self._processed_messages = []

        if not self._messages:
            self.fetch_messages()

        return self

    def next(self):
        """Get the next batch of messages from the previously fetched messages.

        If there's no more messages, check if we should auto-delete the
        messages and raise StopIteration.
        """
        if not self._messages:
            if self.auto_delete:
                self.delete_messages()
            raise StopIteration

        message = self._messages.pop(0)
        self._processed_messages.append(message)
        return json.loads(message.payload)

    def delete_messages(self, only_processed=True):
        """Delete the messages previously leased.

        Unless otherwise directed, only the messages iterated over will be
        deleted.
        """
        messages = self._processed_messages
        if not only_processed:
            messages += self._messages

        if messages:
            try:
                self.queue.delete_tasks(messages)
            except Exception:
                logging.exception("Error deleting messages")
                raise


def bump_batch(work_group):
    """Return the incremented batch id for the work group
    :param work_group: :class: `str`

    :return: :class: `int` current batch id.
    """
    key = "%s-%s" % (MESSAGE_BATCH_NAME, work_group)
    return memcache.incr(key)
