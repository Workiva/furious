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


def example_function(*args, **kwargs):
    logging.info('example_function executed with args: %r, kwargs: %r',
                 args, kwargs)

    return args


class AsyncIntroHandler(webapp2.RequestHandler):
    def get(self):
        """Create and insert a single furious task."""
        from furious.async import Async

        # Instantiate an Async object.
        async_task = Async(
            target=example_function, args=[1], kwargs={'some': 'value'})

        # Insert the task to run the Async object, not that it may begin
        # executing immediately or with some delay.
        async_task.start()

        logging.info('Async job kicked off.')

        self.response.out.write('Successfully inserted Async job.')


class ContextIntroHandler(webapp2.RequestHandler):
    def get(self):
        """Batch insert a group of furious tasks."""
        from furious.async import Async
        from furious import context

        with context.new() as ctx:
            # "Manually" instantiate and add an Async object.
            async_task = Async(
                target=example_function, kwargs={'first': 'async'})
            ctx.add(async_task)
            logging.info('Added manual job to context.')

            for i in xrange(5):
                ctx.add(target=example_function, args=[i])
                logging.info('Added job %d to context.', i)

        logging.info('Async jobs for context batch inserted.')

        self.response.out.write('Successfully inserted a group of Async jobs.')


def all_done():
    """Will be run if the async task runs successfully."""
    from furious.context import get_current_async

    async = get_current_async()

    logging.info('async task complete, value returned: %r', async.result)


class AsyncCallbackHandler(webapp2.RequestHandler):
    def get(self):
        """Create and insert a single furious task."""
        from furious.async import Async

        # Instantiate an Async object.
        async_task = Async(
            target=example_function, args=[1], kwargs={'some': 'value'},
            callbacks={'success': all_done}
        )

        # Insert the task to run the Async object, not that it may begin
        # executing immediately or with some delay.
        async_task.start()

        logging.info('Async job kicked off.')

        self.response.out.write('Successfully inserted Async job.')


app = webapp2.WSGIApplication([
    ('/', AsyncIntroHandler),
    ('/context', ContextIntroHandler),
    ('/callback', AsyncCallbackHandler),
])

