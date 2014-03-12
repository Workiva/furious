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
The ExecutionContext is used to wrap the execution of an Async job to provide
a mechanism to cleanly make information available without the need to pass a
lot of state between functions.

NOTE: Direct usage of this is only needed in very special cases.  Yours
probably is not one of those special cases.

Usage:

    with execution_context_from_async(async) as execution_context:
        run_job()

"""

from furious.context import _local

from furious import errors


__all__ = ["execution_context_from_async"]


def execution_context_from_async(async):
    """Instantiate a new _ExecutionContext and store a reference to it in the
    global async context to make later retrieval easier.
    """
    local_context = _local.get_local_context()

    if local_context._executing_async_context:
        raise errors.ContextExistsError

    execution_context = _ExecutionContext(async)
    local_context._executing_async_context = execution_context
    return execution_context


class _ExecutionContext(object):
    """_ExecutionContext objects are used when running an async task to provide
    easy access to the async object.
    """
    def __init__(self, async):
        """Initialize a context with an async task."""
        from furious.async import Async

        if not isinstance(async, Async):
            raise TypeError("async must be an Async instance.")

        self._async = async
        async.set_execution_context(self)

    @property
    def async(self):
        """Get a reference to this context's async object."""
        return self._async

    def __enter__(self):
        """Enter the context, add this async to the executing context stack."""
        _local.get_local_context()._executing_async.append(self._async)
        return self

    def __exit__(self, *exc_info):
        """Exit the context, pop this async from the executing context stack.
        """
        local_context = _local.get_local_context()
        last = local_context._executing_async.pop()
        if last is not self._async:
            local_context._executing_async.append(last)
            raise errors.CorruptContextError(*exc_info)

        return False

