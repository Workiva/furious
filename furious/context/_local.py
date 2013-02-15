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

"""
This module contains the logic related to the actual request local context
object.

NOTE: This should not typically be used directly by developers.

Usage:

    # Grab a refernce to the local context object.
    local_context = local.get_local_context()

    # Do something with the local context.
    local_context.registry.append(context.new())

"""
import logging

import os
import threading
import uuid

logger = logging.getLogger('_local')
logger.setLevel(logging.DEBUG)


__all__ = ["get_local_context"]


def get_local_context():
    """Return a reference to the current _local_context.

    NOTE: This function is not for general usage, it is meant for
    special uses, like unit tests.
    """
    _init()
    return _local_context


class SeeList(list):
    def __init__(self, *args):
        list.__init__(self, *args)
        self.request_id = os.environ.get('REQUEST_ID_HASH')
        self.guid = uuid.uuid4().hex
        logger.debug("see_list: init {0} {1}".format(self.request_id, self.guid))

    def __delitem__(self, key):
        logger.debug("id: {2}, request_id: {1} see_list: delete {0}".format(key, self.request_id, self.guid))
        return super(SeeList, self).__delitem__(key)

    def __setitem__(self, key, value):
        logger.debug("id: {3}, request_id: {2} see_list: setitem {0}:{1}".format(key, value, self.request_id, self.guid))
        return super(SeeList, self).__setitem__(key, value)

    def __add__(self, other):
        logger.debug("id: {2}, request_id: {1} see_list: add {0}".format(other, self.request_id, self.guid))
        return super(SeeList, self).__add__(other)

    def __getitem__(self, item):
        logger.debug("id: {2}, request_id: {1} see_list: getitem {0}".format(item, self.request_id, self.guid))
        return super(SeeList, self).__getitem__(item)

    def append(self, p_object):
        logger.debug("id: {2}, request_id: {1} see_list: append {0}".format(p_object, self.request_id, self.guid))
        return super(SeeList, self).append(p_object)

    def pop(self, index=None):
        logger.debug("id: {3}, request_id: {2} see_list: {0} pop {1}".format(self, index, self.request_id, self.guid))
        if index is None:
            return super(SeeList, self).pop()

        return super(SeeList, self).pop(index)


_local_context = threading.local()


def _init():
    """Initialize the furious context and registry.

    NOTE: Do not directly run this method.
    """

    # If there is a context and it is initialized to this request,
    # return, otherwise reinitialize the _local_context.
    if (hasattr(_local_context, '_initialized') and
            _local_context._initialized == os.environ['REQUEST_ID_HASH']):
        return

    # Used to track the context object stack.
    _local_context.registry = []

    # Used to provide easy access to the currently running Async job.
    _local_context._executing_async_context = None
    _local_context._executing_async = SeeList()

    # So that we do not inadvertently reinitialize the local context.
    _local_context._initialized = os.environ['REQUEST_ID_HASH']

    return _local_context


# NOTE: Do not import this directly.  If you MUST use this, access it
# through get_local_context.


