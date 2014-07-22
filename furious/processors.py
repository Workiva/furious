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
import logging

from collections import namedtuple

from furious.async import Async
from furious.async import AsyncResult
from furious.context import Context
from furious.context import get_current_async
from furious.errors import Abort
from furious.errors import AbortAndRestart
from furious.job_utils import path_to_reference


AsyncException = namedtuple('AsyncException', 'error args traceback exception')


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

    function = path_to_reference(function_path)

    try:
        async.executing = True
        async.result = AsyncResult(payload=function(*args, **kwargs),
                                   status=AsyncResult.SUCCESS)
    except Abort as abort:
        logging.info('Async job was aborted: %r', abort)
        async.result = AsyncResult(status=AsyncResult.ABORT)

        # QUESTION: In this eventuality, we should probably tell the context we
        # are "complete" and let it handle completion checking.
        _handle_context_completion_check(async)
        return
    except AbortAndRestart as restart:
        logging.info('Async job was aborted and restarted: %r', restart)
        raise
    except Exception as e:
        async.result = AsyncResult(payload=encode_exception(e),
                                   status=AsyncResult.ERROR)

    _handle_results(async_options)
    _handle_context_completion_check(async)


def _handle_results(options):
    """Process the results of executing the Async's target."""
    results_processor = options.get('_process_results')
    if not results_processor:
        results_processor = _process_results

    processor_result = results_processor()
    if isinstance(processor_result, (Async, Context)):
        processor_result.start()


def _handle_context_completion_check(async):
    """If options specifies a completion check function, execute it."""
    checker = async.get_options().get('_context_checker')

    if not checker:
        logging.debug('no checker defined.')
        return

    # Call the context complete checker with the id of this Async.
    checker(async)


def encode_exception(exception):
    """Encode exception to a form that can be passed around and serialized.

    This will grab the stack, then strip off the last two calls which are
    encode_exception and the function that called it.
    """
    import sys
    return AsyncException(unicode(exception),
                          exception.args,
                          sys.exc_info(),
                          exception)


def _process_results():
    """Process the results from an Async job."""
    async = get_current_async()
    callbacks = async.get_callbacks()

    if not isinstance(async.result.payload, AsyncException):
        callback = callbacks.get('success')
    else:
        callback = callbacks.get('error')

        if not callback:
            raise async.result.payload.exception, None, \
                async.result.payload.traceback[2]

    return _execute_callback(async, callback)


def _execute_callback(async, callback):
    """Execute the given callback or insert the Async callback, or if no
    callback is given return the async.result.
    """
    from furious.async import Async

    if not callback:
        return async.result.payload

    if isinstance(callback, Async):
        return callback.start()

    return callback()

