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

"""A very basic async context example.

This example will create a context that adds an Async manually to the context.
It then adds 5 Async jobs through the shorthand style.
"""


import logging

import webapp2

def l_combiner(results):
    return reduce(lambda x, y: x+y,results,0)

def iv_combiner(results):
    return results

class ContextIntroHandler(webapp2.RequestHandler):
    """Demonstrate using a Context to batch insert a
    group of furious tasks."""
    def get(self):
        from furious.async import Async
        from furious import context

        # Create a new furious Context.
        with context.new(callbacks={'internal_vertex_combiner':l_combiner,
                                    'leaf_combiner':l_combiner}) as ctx:
            # "Manually" instantiate and add an Async object to the Context.
            async_task = Async(
                target=example_function, kwargs={'first': 'async'})
            ctx.add(async_task)
            logging.info('Added manual job to context.')

            # Use the shorthand style, note that add returns the Async object.
            for i in xrange(15):
                ctx.add(target=example_function, args=[i])
                logging.info('Added job %d to context.', i)

        # When the Context is exited, the tasks are inserted (if there are no
        # errors).

        logging.info('Async jobs for context batch inserted.')

        self.response.out.write('Successfully inserted a group of Async jobs.')


def example_function(*args, **kwargs):
    """This function is called by furious tasks to demonstrate usage."""
    logging.info('example_function executed with args: %r, kwargs: %r',
                 args, kwargs)
    return l_combiner(args)
