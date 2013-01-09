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
Furious context may be used to group a collection of Async tasks together.

Usage:


    with context.new() as current_context:
        # An explicitly constructed Async object may be passed in.
        async_job = Async(some_function,
                          [args, for, other],
                          {'kwargs': 'for', 'other': 'function'},
                          queue='workgroup')
        current_context.add(async_job)

        # Alternatively, add will construct an Async object when given
        # a function path or reference as the first argument.
        async_job = current_context.add(
            another_function,
            [args, for, other],
            {'kwargs': 'for', 'other': 'function'},
            queue='workgroup')

"""

from . import _local
from .context import Context

from . import _execution


ContextExistsError = _execution.ContextExistsError
CorruptContextError = _execution.CorruptContextError
execution_context_from_async = _execution.execution_context_from_async


class AlreadyInContextError(Exception):
    """Attempt to set context on an Async that is already executing in a
    context.
    """


class NotInContextError(Exception):
    """Call that requires context made outside context."""


def new():
    """Get a new furious context and add it to the registry."""

    new_context = Context()

    _local.get_local_context().registry.append(new_context)

    return new_context


def get_current_async():
    """Return a reference to the currently executing Async job object
    or None if not in an Async job.
    """
    local_context = _local.get_local_context()

    if local_context._executing_async:
        return local_context._executing_async[-1]

    raise NotInContextError('Not in an _ExecutionContext.')

