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

import os
import threading


__all__ = ["get_local_context"]


def get_local_context():
    """Return a reference to the current _local_context.

    NOTE: This function is not for general usage, it is meant for
    special uses, like unit tests.
    """
    _init()
    return _local_context


def _init():
    """Initialize the furious context and registry.

    NOTE: Do not directly run this method.
    """
    # If there is a context and it is initialized to this request,
    # return, otherwise reinitialize the _local_context.
    if (hasattr(_local_context, '_initialized') and
            _local_context._initialized == os.environ.get('REQUEST_ID_HASH')):
        return

    # Used to track the context object stack.
    _local_context.registry = []

    # Used to provide easy access to the currently running Async job.
    _local_context._executing_async_context = None
    _local_context._executing_async = []

    # So that we do not inadvertently reinitialize the local context.
    _local_context._initialized = os.environ.get('REQUEST_ID_HASH')

    return _local_context


def _clear_context():
    """Clear the context.

    Create a clean uninitialized context.
    This is not typically used. It is mainly for use in local tests when
    running asyncs within a single process.
    """

    global _local_context
    _local_context = threading.local()


# NOTE: Do not import this directly.  If you MUST use this, access it
# through get_local_context.
_local_context = threading.local()

