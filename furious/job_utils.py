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
            return ('.'.join((function.__module__, function.func_name)),
                    options)
        except AttributeError:
            if function.__module__ == '__builtin__':
                return function.__name__, options

        raise BadFunctionPathError("Unable to determine path to callable.")

    raise BadFunctionPathError("Must provide a function path or reference.")



        raise BadFunctionPath("Unable to determine path to callable.")

    raise BadFunctionPath("Must provide a function path or reference.")

