"""
Functions to help with encoding and decoding job information.
"""


class BadFunctionPath(Exception):
    """Invalid function path."""

class InvalidJobTuple(Exception):
    """Invalid job tuple."""


def check_job(job):
    """Ensure job is of an expected form and return the standard format.

    Note: This is an amazingly ugly function.
    """
    if isinstance(job, basestring) or callable(job):
        # This is probably a function with no arguments.
        return (_check_job_function(job), None, None)

    if len(job) == 1:
        # This might be a function only job tuple.
        return (_check_job_function(job[0]), None, None)

    # First part of the job should be the method str or callable.
    method_path = _check_job_function(job[0])

    if len(job) == 3:
        # Hopefully of the form (method, *args, **kwargs), but lets check.

        _, args, kwargs = job

        if ((isinstance(args, (list, tuple)) or args is None) and
            (isinstance(kwargs, dict) or kwargs is None)):
            # We seem to be good.
            return (method_path, args, kwargs)

    if len(job) != 2:
        raise InvalidJobTuple(
            "Job tuple should be of the form (job, args, kwrgs)")

    mystery_part = job[-1]
    if isinstance(mystery_part, dict):
        # assume we're given a kwargs dict.
        return (method_path, None, mystery_part)

    if isinstance(mystery_part, (list, tuple)):
        # assume mystery_part is args.
        return (method_path, mystery_part, None)

    raise InvalidJobTuple("Poorly formed job tuple.  Job tuple should be of "
                          "the form (job, args, kwrgs)")


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

