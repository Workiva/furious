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

import logging

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
    pass


def context_completion_checker(async):
    """Check if all Async jobs within a Context have been run."""
    context_id = async.context_id
    logging.debug("Check completion for: %s", context_id)

    context = FuriousContext.from_id(context_id)
    logging.debug("Loaded context.")

    task_ids = context.task_ids
    logging.debug(task_ids)

    offset = 10
    for index in xrange(0, len(task_ids), offset):
        keys = [ndb.Key(FuriousAsyncMarker, id)
                for id in task_ids[index:index + offset]]

        markers = ndb.get_multi(keys)

        if not all(markers):
            logging.debug("Not all Async's complete")
            return False

    logging.debug("All Async's complete!!")

    context.exec_event_handler('complete')

    return True


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
    pass


def store_async_marker(async):
    """Persist a marker indicating the Async ran to the datastore."""
    logging.debug("Attempting to mark Async %s complete.", async.id)

    # TODO: Handle exceptions and retries here.
    key = FuriousAsyncMarker(id=async.id).put()

    logging.debug("Marked Async complete using marker: %s.", key)

