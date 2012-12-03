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

"""These functions are used to run an Async job.  These are the "real" worker
functions.
"""

from .job_utils import function_path_to_reference

from collections import namedtuple


AsyncException = namedtuple('AsyncException', 'error args traceback exception')


class AsyncError(Exception):
    """The base class other Async errors can subclass."""


def run_job():
    """Takes an async object and executes its job."""
    from .context import get_current_async

    async = get_current_async()

    async_options = async.get_options()

    job = async_options.get('job')
    if not job:
        raise Exception('This async contains no job to execute!')

    function_path, args, kwargs = job

    if args is None:
        args = ()

    if kwargs is None:
        kwargs = {}

    function = function_path_to_reference(function_path)

    try:
        async.executing = True
        async.result = function(*args, **kwargs)
    except Exception as e:
        async.result = encode_exception(e)

    results_processor = async_options.get('_process_results')
    if not results_processor:
        results_processor = _process_results

    results_processor()


def encode_exception(exception):
    """Encode exception to a form that can be passed around and serialized."""
    import traceback
    return AsyncException(unicode(exception),
                          exception.args,
                          traceback.extract_stack()[:-2],
                          exception)


def _process_results():
    """Process the results from an Async job."""
    from .context import get_current_async
    current_job = get_current_async()
    callbacks = current_job.get_callbacks()

    if isinstance(current_job.result, AsyncException):
        error_callback = callbacks.get('error')
        if not error_callback:
            return
        error_callback()
        return

    success_callback = callbacks.get('success')
    if not success_callback:
        return
    success_callback()


