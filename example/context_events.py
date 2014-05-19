#
# Copyright 2014 WebFilings, LLC
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

"""Example of using Context events for work flow.

This example creates a context with events, then adds a few Async jobs to the
Context.
"""


import logging

import webapp2


class ContextEventsHandler(webapp2.RequestHandler):
    """Demonstrate using Context Events to make work flows."""
    def get(self):
        from furious.async import Async
        from furious import context

        count = int(self.request.get('tasks', 5))

        # Create a new furious Context.
        with context.new() as ctx:
            # Set a completion event handler.
            ctx.set_event_handler('complete',
                                  Async(context_complete, args=[ctx.id]))

            # Insert some Asyncs.
            for i in xrange(count):
                ctx.add(target=async_worker, args=[ctx.id, i])
                logging.info('Added job %d to context.', i)

        # When the Context is exited, the tasks are inserted (if there are no
        # errors).

        logging.info('Async jobs for context batch inserted.')

        self.response.out.write('Successfully inserted a group of Async jobs.')


def async_worker(*args, **kwargs):
    """This function is called by furious tasks to demonstrate usage."""
    logging.info('Context %s, function %s', *args)

    return args


def context_complete(context_id):
    """Log out that the context is complete."""
    logging.info('Context %s is.......... DONE.', context_id)

    return context_id

