"""
Functions to help with encoding and decoding job information.
"""


class BadFunctionPath(Exception):
    """Invalid function path."""

class InvalidJobTuple(Exception):
    """Invalid job tuple."""





def _check_job_function(function):
    """Validates `function` is a potentially valid path or reference to
    a function and returns the cleansed path to the function.

    Strings are checked to ensure they conform to Python's identifier
    requirements:
        http://docs.python.org/reference/lexical_analysis.html#identifiers

    Functions passed by reference must be at the top-level of the containing
    module.
    """
    if isinstance(function, basestring):
        # This is a function name in str form.
        import re
        if not re.match(r'^[^\d\W]([a-zA-Z._]|((?<!\.)\d))+$', function):
            raise BadFunctionPath(
                'Invalid function path, must meet Python\'s identifier '
                'requirements, passed value was "%s".', function)
        return function

    if callable(function):
        # Try to figure out the path to the function.
        try:
            return '.'.join((function.__module__, function.func_name))
        except AttributeError:
            if function.__module__ == '__builtin__':
                return function.__name__

        raise BadFunctionPath("Unable to determine path to callable.")

    raise BadFunctionPath("Must provide a function path or reference.")

