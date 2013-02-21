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

from collections import namedtuple

from .async import AbortAndRestart
from .async import Async
from .context import Context
from .context import get_current_async
from furious.context import _local
from furious.marker_tree.async_utils import handle_async_done
from .job_utils import function_path_to_reference


AsyncException = namedtuple('AsyncException', 'error args traceback exception')


class AsyncError(Exception):
    """The base class other Async errors can subclass."""


def run_job():
    """Takes an async object and executes its job."""
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
    except AbortAndRestart as restart:
        import logging
        logging.info('Async job was aborted and restarted: %r', restart)
        async._restart()
        return
    except Exception as e:
        async.result = encode_exception(e)

    results_processor = async_options.get('_process_results')
    if not results_processor:
        results_processor = _process_results

    processor_result = results_processor()
    if isinstance(processor_result, (Async, Context)):

        # Use async's id so the context's
        # success callback gets hit when the this
        # next async is done.
        processor_result.id = async.id

        if isinstance(processor_result, Context):
            _local.get_local_context().registry.append(processor_result)
            # If the context contained no tasks go ahead and
            # mark this node as done and bubble up.
            if not processor_result.will_completion_run():
                # Clear the empty Context from the async result.
                async._executing = True
                async.result = None
                async._executing = False
                handle_async_done(async)
            else:
                processor_result.start()
        else:
            processor_result.start()
    else:
        handle_async_done(async)


def encode_exception(exception):
    """Encode exception to a form that can be passed around and serialized.

    This will grab the stack, then strip off the last two calls which are
    encode_exception and the function that called it.
    """
    import traceback
    return AsyncException(unicode(exception),
                          exception.args,
                          traceback.extract_stack()[:-2],
                          exception)


def _process_results():
    """Process the results from an Async job."""
    async = get_current_async()
    callbacks = async.get_callbacks()

    if isinstance(async.result, AsyncException):
        error_callback = callbacks.get('error')
        if not error_callback:
            raise async.result.exception

        return _execute_callback(async, error_callback)

    success_callback = callbacks.get('success')
    return _execute_callback(async, success_callback)


def _execute_callback(async, callback):
    """Execute the given callback or insert the Async callback, or if no
    callback is given return the async.result.
    """
    from furious.async import Async

    if not callback:
        return async.result

    if isinstance(callback, Async):
        return callback.start()

    return callback()

