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
Functions to help with encoding and decoding job information.
"""

import sys


class BadFunctionPathError(Exception):
    """Invalid function path."""


def get_function_path_and_options(function):
    """Validates `function` is a potentially valid path or reference to
    a function and returns the cleansed path to the function.

    Strings are checked to ensure they conform to Python's identifier
    requirements:
        http://docs.python.org/reference/lexical_analysis.html#identifiers

    Functions passed by reference must be at the top-level of the containing
    module.

    Returns the function path and options.
    """
    # Try to pop the options off whatever they passed in.
    options = getattr(function, '_async_options', None)

    if isinstance(function, basestring):
        # This is a function name in str form.
        import re
        if not re.match(r'^[^\d\W]([a-zA-Z._]|((?<!\.)\d))+$', function):
            raise BadFunctionPathError(
                'Invalid function path, must meet Python\'s identifier '
                'requirements, passed value was "%s".', function)
        return function, options

    if callable(function):
        # Try to figure out the path to the function.
        try:
            parts = [function.__module__]
            if hasattr(function, 'im_class'):
                parts.append(function.im_class.__name__)
            parts.append(function.func_name)

            return ('.'.join(parts), options)
        except AttributeError:
            if function.__module__ == '__builtin__':
                return function.__name__, options

        raise BadFunctionPathError("Unable to determine path to callable.")

    raise BadFunctionPathError("Must provide a function path or reference.")


def path_to_reference(path):
    """Convert a function path reference to a reference."""

    # By default JSON decodes strings as unicode. The Python __import__ does
    # not like that choice. So we'll just cast all function paths to a string.
    # NOTE: that there is no corresponding unit test for the classmethod
    # version of this problem.  It only impacts importing modules.
    path = str(path)

    if '.' not in path:
        try:
            return globals()["__builtins__"][path]
        except KeyError:
            try:
                return getattr(globals()["__builtins__"], path)
            except AttributeError:
                pass

        try:
            return globals()[path]
        except KeyError:
            pass

        raise BadFunctionPathError(
            'Unable to find function "%s".' % (path,))

    module_path, function_name = path.rsplit('.', 1)

    if module_path in sys.modules:
        module = sys.modules[module_path]
    else:
        try:
            module = __import__(name=module_path,
                                fromlist=[function_name])
        except ImportError:
            module_path, class_name = module_path.rsplit('.', 1)

            module = __import__(name=module_path, fromlist=[class_name])
            module = getattr(module, class_name)

    try:
        return getattr(module, function_name)
    except AttributeError:
        raise BadFunctionPathError(
            'Unable to find function "%s".' % (path,))


def encode_callbacks(callbacks):
    """Encode callbacks to as a dict suitable for JSON encoding."""
    from .async import Async

    if not callbacks:
        return

    encoded_callbacks = {}
    for event, callback in callbacks.iteritems():
        if callable(callback):
            callback, _ = get_function_path_and_options(callback)

        elif isinstance(callback, Async):
            callback = callback.to_dict()

        encoded_callbacks[event] = callback

    return encoded_callbacks


def decode_callbacks(encoded_callbacks):
    """Decode the callbacks to an executable form."""
    from .async import Async

    callbacks = {}
    for event, callback in encoded_callbacks.iteritems():
        if isinstance(callback, dict):
            callback = Async.from_dict(callback)
        else:
            callback = path_to_reference(callback)

        callbacks[event] = callback

    return callbacks

