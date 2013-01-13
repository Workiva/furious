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

"""Contained within this module are several working examples showing basic
usage and complex context-based chaining.

The examples demonstrate basic task execution, and also the basics of creating
more complicated processing pipelines.
"""

import logging

import webapp2

from furious.async import defaults


class AsyncIntroHandler(webapp2.RequestHandler):
    """Demonstrate the creation and insertion of a single furious task."""
    def get(self):
        from furious.async import Async

        # Instantiate an Async object.
        async_task = Async(
            target=whatup, args=[100])

        # Insert the task to run the Async object, note that it may begin
        # executing immediately or with some delay.
        async_task.start()

        logging.info('Async job kicked off.')

        self.response.out.write('Successfully inserted Async job.')


class ContextIntroHandler(webapp2.RequestHandler):
    """Demonstrate using a Context to batch insert a group of furious tasks."""
    def get(self):
        from furious.async import Async
        from furious import context
        from random import randint

        # Create a new furious Context.
        with context.new() as ctx:
            # "Manually" instantiate and add an Async object to the Context.
            ctx.add(target=whatup, args=[randint(1,20)])#,callbacks={'success': oooeee})
            ctx.add(target=whatup, args=[randint(1,20)])#,callbacks={'success': oooeee})
            ctx.add(target=whatup, args=[randint(1,20)])#,callbacks={'success': oooeee})
            ctx.add(target=whatup, args=[randint(1,20)])#,callbacks={'success': oooeee})
            ctx.add(target=whatup, args=[randint(1,20)])#,callbacks={'success': oooeee})

            logging.info('Added manual job to context.')

            # Use the shorthand style, note that add returns the Async object.
            for i in xrange(55):
                ctx.add(target=whatup, args=[i])
                logging.info('Added job %d to context.', i)

        # When the Context is exited, the tasks are inserted (if there are no
        # errors).

        persistence_id = ctx._persistence_id

        logging.info('Async jobs for context batch inserted.')

        self.response.out.write('Successfully inserted a group of Async jobs. with persistence_id %s'%persistence_id)


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


class SimpleWorkflowHandler(webapp2.RequestHandler):
    """Demonstrate constructing a simple state machine."""
    def get(self):
        from furious.async import Async

        # Instantiate an Async object to start the chain.
        Async(target=simple_state_machine).start()

        logging.info('Async chain kicked off.')

        self.response.out.write('Successfully inserted Async chain starter.')


class ComplexWorkflowHandler(webapp2.RequestHandler):
    """Demonstrate constructing a more complex state machine."""
    def get(self):
        from furious.async import Async

        # Instantiate an Async object to start the machine in state alpha.
        Async(target=complex_state_generator_alpha).start()

        logging.info('Async chain kicked off.')

        self.response.out.write('Successfully inserted Async chain starter.')


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

    exception_info = get_current_async().result

    logging.info('async job blew up, exception info: %r', exception_info)

    retries = int(os.environ['HTTP_X_APPENGINE_TASKRETRYCOUNT'])
    if retries < 2:
        raise exception_info.exception
    else:
        logging.info('Caught too many errors, giving up now.')


def simple_state_machine():
    """Pick a number, if it is more than some cuttoff continue the chain."""
    from random import random

    from furious.async import Async

    number = random()
    logging.info('Generating a number... %s', number)

    if number > 0.25:
        logging.info('Continuing to do stuff.')
        return Async(target=simple_state_machine)

    return number


@defaults(callbacks={'success': "example.state_machine_success"})
def complex_state_generator_alpha(last_state=''):
    """Pick a state."""
    from random import choice

    states = ['ALPHA', 'ALPHA', 'ALPHA', 'BRAVO', 'BRAVO', 'DONE']
    if last_state:
        states.remove(last_state)  # Slightly lower chances of previous state.

    state = choice(states)

    logging.info('Generating a state... %s', state)

    return state


@defaults(callbacks={'success': "example.state_machine_success"})
def complex_state_generator_bravo(last_state=''):
    """Pick a state."""
    from random import choice

    states = ['ALPHA', 'BRAVO', 'BRAVO', 'DONE']
    if last_state:
        states.remove(last_state)  # Slightly lower chances of previous state.

    state = choice(states)

    logging.info('Generating a state... %s', state)

    return state


def state_machine_success():
    """A positive result!  Iterate!"""
    from furious.async import Async
    from furious.context import get_current_async

    result = get_current_async().result

    if result == 'ALPHA':
        logging.info('Inserting continuation for state %s.', result)
        return Async(target=complex_state_generator_alpha, args=[result])

    elif result == 'BRAVO':
        logging.info('Inserting continuation for state %s.', result)
        return Async(target=complex_state_generator_bravo, args=[result])

    logging.info('Done working, stop now.')


def whatup(num):
    return num * (num +1)/2.0

def oooeee():
    from furious.async import Async
    from furious.context import get_current_async

    result = get_current_async().result

    logging.info('async task complete, value returned: %r', result)
    if 1e+40 > result > 1:
        return Async(target=whatup, args=[result],callbacks={'success': oooeee})
    else:
        logging.info("whoa!")

app = webapp2.WSGIApplication([
    ('/', AsyncIntroHandler),
    ('/context', ContextIntroHandler),
    ('/callback', AsyncCallbackHandler),
    ('/callback/error', AsyncErrorCallbackHandler),
    ('/callback/async', AsyncAsyncCallbackHandler),
    ('/workflow', SimpleWorkflowHandler),
    ('/workflow/complex', ComplexWorkflowHandler),
])

