#
# Copyright 2014 WebFilings, LLC
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
"""This module contains the default functions to use when performing
persistence operations backed by the App Engine ndb library.
"""
import json
import logging
import os

from itertools import imap
from itertools import islice
from itertools import izip

from random import shuffle

from google.appengine.ext import ndb

from furious.context.context import ContextResultBase
from furious import config

CLEAN_QUEUE = config.get_completion_cleanup_queue()
DEFAULT_QUEUE = config.get_completion_default_queue()
CLEAN_DELAY = config.get_completion_cleanup_delay()
QUEUE_HEADER = 'HTTP_X_APPENGINE_QUEUENAME'


class FuriousContextNotFoundError(Exception):
    """FuriousContext entity not found in the datastore."""


class FuriousContext(ndb.Model):
    """NDB entity to store a Furious Context as JSON."""

    context = ndb.JsonProperty(indexed=False, compressed=True)

    @classmethod
    def from_context(cls, context):
        """Create a `cls` entity from a context."""
        return cls(id=context.id, context=context.to_dict())

    @classmethod
    def from_id(cls, id):
        """Load a `cls` entity and instantiate the Context it stores."""
        from furious.context import Context

        # TODO: Handle exceptions and retries here.
        entity = cls.get_by_id(id)
        if not entity:
            raise FuriousContextNotFoundError(
                "Context entity not found for: {}".format(id))

        return Context.from_dict(entity.context)


class FuriousAsyncMarker(ndb.Model):
    """This entity serves as a 'complete' marker."""

    result = ndb.JsonProperty(indexed=False, compressed=True)
    status = ndb.IntegerProperty(indexed=False)

    @property
    def success(self):
        from furious.async import AsyncResult
        return self.status != AsyncResult.ERROR


class FuriousCompletionMarker(ndb.Model):
    """This entity serves as a 'complete' marker for the entire context."""

    complete = ndb.BooleanProperty(default=False, indexed=False)
    has_errors = ndb.BooleanProperty(default=False, indexed=False)


class ContextResult(ContextResultBase):

    BATCH_SIZE = 10

    def __init__(self, context):
        self._context = context
        self._task_cache = {}
        self._marker = None

    @property
    def _tasks(self):
        if self._task_cache:
            return ((key, task) for key, task in self._task_cache.iteritems())

        return iter_context_results(self._context, self.BATCH_SIZE,
                                    self._task_cache)

    @property
    def _completion_marker(self):
        if not self._marker:
            self._marker = FuriousCompletionMarker.get_by_id(
                self._context.id)

        return self._marker

    def items(self):
        """Yield the async reuslts for the context."""
        for key, task in self._tasks:
            if not (task and task.result):
                yield key, None
            else:
                yield key, json.loads(task.result)["payload"]

    def values(self):
        """Yield the async reuslt values for the context."""
        for _, task in self._tasks:
            if not (task and task.result):
                yield None
            else:
                yield json.loads(task.result)["payload"]

    def has_errors(self):
        """Return the error flag from the completion marker."""
        if self._completion_marker:
            return self._completion_marker.has_errors

        return False


def context_completion_checker(async):
    """Persist async marker and async the completion check"""

    store_async_marker(async.id, async.result.status if async.result else -1)

    logging.debug("Async check completion for: %s", async.context_id)
    current_queue = _get_current_queue()
    from furious.async import Async
    logging.debug("Completion Check queue:%s", current_queue)
    Async(_completion_checker, queue=current_queue,
          args=(async.id, async.context_id)).start()

    return True


def _get_current_queue():

    return os.environ.get(QUEUE_HEADER, DEFAULT_QUEUE)


def _completion_checker(async_id, context_id):
    """Check if all Async jobs within a Context have been run."""

    if not context_id:
        logging.debug("Context for async %s does not exist", async_id)
        return

    context = FuriousContext.from_id(context_id)
    marker = FuriousCompletionMarker.get_by_id(context_id)

    if marker and marker.complete:
        logging.info("Context %s already complete" % context_id)
        return True

    task_ids = context.task_ids
    if async_id in task_ids:
        task_ids.remove(async_id)

    logging.debug("Loaded context.")
    logging.debug(task_ids)

    done, has_errors = _check_markers(task_ids)

    if not done:
        return False

    _mark_context_complete(marker, context, has_errors)

    return True


def _check_markers(task_ids, offset=10):
    """Returns a flag for markers being found for the task_ids. If all task ids
    have markers True will be returned. Otherwise it will return False as soon
    as a None result is hit.
    """

    shuffle(task_ids)
    has_errors = False

    for index in xrange(0, len(task_ids), offset):
        keys = [ndb.Key(FuriousAsyncMarker, id)
                for id in task_ids[index:index + offset]]

        markers = ndb.get_multi(keys)

        if not all(markers):
            logging.debug("Not all Async's complete")
            return False, None

        # Did any of the aync's fail? Check the success property on the
        # AsyncResult.
        has_errors = not all((marker.success for marker in markers))

    return True, has_errors


@ndb.transactional
def _mark_context_complete(marker, context, has_errors):
    """Transactionally 'complete' the context."""

    current = None

    if marker:
        current = marker.key.get()

    if not current:
        return False

    if current and current.complete:
        return False

    current.complete = True
    current.has_errors = has_errors
    current.put()

    # Kick off completion tasks.
    _insert_post_complete_tasks(context)

    return True


def _insert_post_complete_tasks(context):
    """Insert the event's asyncs and cleanup tasks."""

    logging.debug("Context %s is complete.", context.id)

    # Async event handlers
    context.exec_event_handler('complete', transactional=True)

    # Insert cleanup tasks
    try:
        # TODO: If tracking results we may not want to auto cleanup and instead
        # wait until the results have been accessed.
        from furious.async import Async
        Async(_cleanup_markers, queue=CLEAN_QUEUE,
              args=[context.id, context.task_ids],
              task_args={'countdown': CLEAN_DELAY}).start()
    except:
        pass


def _cleanup_markers(context_id, task_ids):
    """Delete the FuriousAsyncMarker entities corresponding to ids."""

    logging.debug("Cleanup %d markers for Context %s",
                  len(task_ids), context_id)

    # TODO: Handle exceptions and retries here.
    delete_entities = [ndb.Key(FuriousAsyncMarker, id) for id in task_ids]
    delete_entities.append(ndb.Key(FuriousCompletionMarker, context_id))

    ndb.delete_multi(delete_entities)

    logging.debug("Markers cleaned.")


def load_context(id):
    """Load a Context object by it's id."""

    return FuriousContext.from_id(id)


def store_context(context):
    """Persist a furious.context.Context object to the datastore by loading it
    into a FuriousContext ndb.Model.
    """

    logging.debug("Attempting to store Context %s.", context.id)

    entity = FuriousContext.from_context(context)

    # TODO: Handle exceptions and retries here.
    marker = FuriousCompletionMarker(id=context.id)
    key, _ = ndb.put_multi((entity, marker))

    logging.debug("Stored Context with key: %s.", key)

    return key


def store_async_result(async_id, async_result):
    """Persist the Async's result to the datastore."""

    logging.debug("Storing result for %s", async_id)

    key = FuriousAsyncMarker(
        id=async_id, result=json.dumps(async_result.to_dict()),
        status=async_result.status).put()

    logging.debug("Setting Async result %s using marker: %s.", async_result,
                  key)


def store_async_marker(async_id, status):
    """Persist a marker indicating the Async ran to the datastore."""

    logging.debug("Attempting to mark Async %s complete.", async_id)

    # QUESTION: Do we trust if the marker had a flag result to just trust it?
    marker = FuriousAsyncMarker.get_by_id(async_id)

    if marker:
        logging.debug("Marker already exists for %s.", async_id)
        return

    # TODO: Handle exceptions and retries here.
    key = FuriousAsyncMarker(id=async_id, status=status).put()

    logging.debug("Marked Async complete using marker: %s.", key)


def iter_context_results(context, batch_size=10, task_cache=None):
    """Yield out the results found on the markers for the context task ids."""

    for futures in iget_batches(context.task_ids, batch_size=batch_size):
        for key, future in futures:
            task = future.get_result()

            if task_cache is not None:
                task_cache[key.id()] = task

            yield key.id(), task


def iget_batches(task_ids, batch_size=10):
    """Yield out a map of the keys and futures in batches of the batch size
    passed in.
    """

    make_key = lambda _id: ndb.Key(FuriousAsyncMarker, _id)
    for keys in i_batch(imap(make_key, task_ids), batch_size):
        yield izip(keys, ndb.get_multi_async(keys))


def i_batch(items, size):
    """Generator that iteratively batches items to a max size and consumes the
    items as each batch is yielded.
    """
    for items_batch in iter(lambda: tuple(islice(items, size)),
                            tuple()):
        yield items_batch


def get_context_result(context):
    return ContextResult(context)
