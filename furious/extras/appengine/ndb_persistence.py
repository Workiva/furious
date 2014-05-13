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

from itertools import izip_longest
from itertools import imap

from google.appengine.ext import ndb


class FuriousContextNotFoundError(Exception):
    """FuriousContext entity not found in the datastore."""


class FuriousContext(ndb.Model):
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


def context_completion_checker(async):
    """Check if all Async jobs within a Context have been run."""

    # Now, check for other Async markers in this Context.
    context_id = async.context_id
    logging.debug("Check completion for: %s", context_id)

    context = FuriousContext.from_id(context_id)
    logging.debug("Loaded context.")

    task_ids = context.task_ids
    task_ids.remove(async.id)

    logging.debug(task_ids)

    offset = 10
    for index in xrange(0, len(task_ids), offset):
        keys = [ndb.Key(FuriousAsyncMarker, id)
                for id in task_ids[index:index + offset]]

        markers = ndb.get_multi(keys)

        if not all(markers):
            logging.debug("Not all Async's complete")
            store_async_marker(async)
            return False

    logging.debug("All Async's complete!!")

    context.exec_event_handler('complete')

    try:
        from furious.async import Async
        Async(_cleanup_markers, args=[context_id, task_ids]).start()
    except:
        pass

    return True


def _cleanup_markers(context_id, task_ids):
    """Delete the FuriousAsyncMarker entities corresponding to ids."""
    logging.debug("Cleanup %d markers for Context %s",
                  len(task_ids), context_id)

    # TODO: Handle exceptions and retries here.
    ndb.delete_multi([ndb.Key(FuriousAsyncMarker, id) for id in task_ids])

    logging.debug("Markers cleaned.")


def load_context(id):
    """Load a Context object by it's id."""
    return FuriousContext.from_id(id)


def store_context(context):
    """Persist a Context object to the datastore."""
    logging.debug("Attempting to store Context %s.", context.id)

    entity = FuriousContext.from_context(context)

    # TODO: Handle exceptions and retries here.
    key = entity.put()

    logging.debug("Stored Context with key: %s.", key)


def store_async_result(async):
    """Persist the Async's result to the datastore."""
    logging.debug("Storing result for %s", async)

    key = FuriousAsyncMarker(
        id=async.id, result=json.dumps(async.result)).put()

    logging.debug("Setting Async result %s using marker: %s.", async.result,
                  key)


def store_async_marker(async):
    """Persist a marker indicating the Async ran to the datastore."""
    logging.debug("Attempting to mark Async %s complete.", async.id)

    # QUESTION: Do we trust if the marker had a flag result to just trust it?
    marker = FuriousAsyncMarker.get_by_id(async.id)

    if marker:
        logging.debug("Marker already exists for %s.", async.id)
        return

    # TODO: Handle exceptions and retries here.
    key = FuriousAsyncMarker(id=async.id).put()

    logging.debug("Marked Async complete using marker: %s.", key)


def iter_results(context):
    for futures in iget_batches(context.task_ids):
        for future in futures:
            task = future.get_result()

            if not (task and task.result):
                yield

            yield json.loads(task.result)


def iget_batches(task_ids, batch_size=10):
    make_key = lambda _id: ndb.Key(FuriousAsyncMarker, _id)
    key_batches = izip_longest(*[imap(make_key, task_ids)] * batch_size)

    for keys in key_batches:
        yield ndb.get_multi_async(filter(None, keys))
