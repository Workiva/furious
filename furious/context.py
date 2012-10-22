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

import threading

_local_context = threading.local()


def new():
    """Get a new furious context and add it to the registry."""
    _init()
    new_context = Context()
    _local_context.registry.append(new_context)
    return new_context


class Context(object):
    """Furious context object.

    NOTE: Use the module's new function to get a context, do not manually
    instantiate.
    """
    def __init__(self):
        self._tasks = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def add(self, target, args=None, kwargs=None, **options):
        """Add an Async job to this context.

        Takes an Async object or the argumnets to construct an Async
        object as argumnets.  Returns the newly added Async object.
        """
        from .async import Async
        if not isinstance(target, Async):
            target = Async(target, args, kwargs, **options)

        self._tasks.append(target)
        return target


def _init():
    """Initialize the furious context and registry.

    NOTE: Do not directly run this method.
    """
    if hasattr(_local_context, '_initialized'):
        return

    _local_context.registry = []
    _local_context._initialized = True

    return _local_context

_init()  # Initialize the context objects.

