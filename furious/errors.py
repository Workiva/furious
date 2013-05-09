#
# Copyright 2013 WebFilings, LLC
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


class BadObjectPathError(Exception):
    """Invalid object path."""


class AsyncError(Exception):
    """The base class other Async errors can subclass."""


class NotExecutedError(Exception):
    """This Async has not yet been executed."""


class NotExecutingError(Exception):
    """This Async in not currently executing."""


class AlreadyExecutedError(Exception):
    """This Async has already been executed."""


class AlreadyExecutingError(Exception):
    """This Async is currently executing."""


class Abort(Exception):
    """This Async needs to be aborted immediately. Only an info level logging
    message will be output about the aborted job.
    """


class AbortAndRestart(Exception):
    """This Async needs to be aborted immediately and restarted."""


class AsyncRecursionError(Abort):
    """This Async has hit the max recursion depth, it should be aborted."""


class ContextAlreadyStartedError(Exception):
    """Attempt to set context on an Async that is already executing in a
    context.
    """


class ContextExistsError(Exception):
    """Call made within context that should not be."""


class CorruptContextError(Exception):
    """ExecutionContext raised when the execution context stack is corrupted.
    """
    def __init__(self, *exc_info):
        self.exc_info = exc_info


class AlreadyInContextError(Exception):
    """Attempt to set context on an Async that is already executing in a
    context.
    """


class NotInContextError(Exception):
    """Call that requires context made outside context."""



