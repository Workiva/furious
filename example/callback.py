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

"""Async Callback Examples.

There are 3 examples below.

1. AsyncCallbackHandler
Registering a function as a callback to be triggered when the job has
completed.

2. AsyncErrorCallbackHandler
Registering a function as an error callback to be triggered when an error has
been hit in the Async process.

3. AsyncAsyncCallbackHandler
Registering another Async object as a callback to be inserted when the job has
complated.
"""


import logging

import webapp2


class AsyncCallbackHandler(webapp2.RequestHandler):
    """Demonstrate setting an Async callback."""
    def get(self):
        from furious.async import Async

        # Instantiate an Async object, specifying a 'success' callback.
        async_task = Async(
            target=example_function, args=[1], kwargs={'some': 'value'},
            callbacks={'success': all_done}
        )

        # Insert the task to run the Async object.  The success callback will
        # be executed in the furious task after the job is executed.
        async_task.start()

        logging.info('Async job kicked off.')

        self.response.out.write('Successfully inserted Async job.')


class AsyncErrorCallbackHandler(webapp2.RequestHandler):
    """Demonstrate handling an error using an Async callback."""
    def get(self):
        from furious.async import Async

        # Instantiate an Async object, specifying a 'error' callback.
        async_task = Async(
            target=dir, args=[1, 2, 3],
            callbacks={'error': handle_an_error}
        )

        # Insert the task to run the Async object.  The error callback will be
        # executed in the furious task after the job has raised an exception.
        async_task.start()

        logging.info('Erroneous Async job kicked off.')

        self.response.out.write('Successfully inserted Async job.')


class AsyncAsyncCallbackHandler(webapp2.RequestHandler):
    """Demonstrate using an Async as a callback for another Async."""
    def get(self):
        from furious.async import Async

        # Instantiate an Async object to act as our success callback.
        # NOTE: Your async.result is not directly available from the
        # success_callback Async, you will need to persist the result
        # and fetch it from the other Async if needed.
        success_callback = Async(
            target=example_function, kwargs={'it': 'worked'}
        )

        # Instantiate an Async object, setting the success_callback to the
        # above Async object.
        async_task = Async(
            target=example_function, kwargs={'trigger': 'job'},
            callbacks={'success': success_callback}
        )

        # Insert the task to run the Async object.
        async_task.start()

        logging.info('Async job kicked off.')

        self.response.out.write('Successfully inserted Async job.')


def example_function(*args, **kwargs):
    """This function is called by furious tasks to demonstrate usage."""
    logging.info('example_function executed with args: %r, kwargs: %r',
                 args, kwargs)

    return args


def all_done():
    """Will be run if the async task runs successfully."""
    from furious.context import get_current_async

    async = get_current_async()

    logging.info('async task complete, value returned: %r', async.result)


def handle_an_error():
    """Will be run if the async task raises an unhandled exception."""
    import os

    from furious.context import get_current_async

    async = get_current_async()
    async_exception = async.result.payload
    exc_info = async_exception.traceback
    logging.info('async job blew up, exception info: %r', exc_info)

    retries = int(os.environ['HTTP_X_APPENGINE_TASKRETRYCOUNT'])
    if retries < 2:
        raise Exception(async_exception.error)
    else:
        logging.info('Caught too many errors, giving up now.')
