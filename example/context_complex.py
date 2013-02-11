#
# Copyright 2013 WebFilings, LLC
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
from google.appengine.api import memcache

import webapp2
from furious.extras.appengine.ndb import Result

class ContextComplexHandler(webapp2.RequestHandler):
    """Demonstrate using a Context to batch insert a
    group of furious tasks."""
    def get(self):
        from furious.async import Async
        from furious import context

        # Create a new furious Context.
        with context.new(callbacks={'internal_vertex_combiner':l_combiner,
                                    'leaf_combiner':l_combiner,
                                    'success':example_callback_success}) as ctx:
            # "Manually" instantiate and add an Async object to the Context.
            async_task = Async(
                target=example_function, kwargs={'first': 'async'})
            ctx.add(async_task)
            logging.info('Added manual job to context.')

            # instantiate and add an Async who's function creates another Context.
            # enabling extra fan-out of a job
            async_task = Async(
                target=make_a_new_context_example, kwargs={'extra': 'async'})
            ctx.add(async_task)
            logging.info('Added sub context')

            # Use the shorthand style, note that add returns the Async object.
            for i in xrange(25):
                ctx.add(target=example_function, args=[i])
                logging.info('Added job %d to context.', i)


            # Instantiate and add an Async who's function creates another Async
            # enabling portions of the job to be serial
            async_task = Async(
                target=make_a_new_async_example, kwargs={'second': 'async'})
            ctx.add(async_task)
            logging.info('Added added async that returns an async')



        # When the Context is exited, the tasks are inserted (if there are no
        # errors).

        logging.info('Async jobs for context batch inserted.')

        self.response.out.write('Successfully inserted a '
        'group of Async jobs with Furious:{0}'.format(ctx.id))

def l_combiner(results):
    return reduce(lambda x, y: x+y,results,0)

def iv_combiner(results):
    return results

def example_callback_success(id,result):
    result = Result(
        id=id,
        result=result)
    result.put()
    memcache.set("Furious:{0}".format(id), "done by callback")

def example_function(*args, **kwargs):
    """This function is called by furious tasks to demonstrate usage."""
    logging.info('example_function executed with args: %r, kwargs: %r',
                 args, kwargs)
    return l_combiner(args)

def make_a_new_async_example(*args, **kwargs):
    from furious.async import Async
    async_task = Async(
        target=example_function,args=[500])

    return async_task

def make_a_new_context_example(*args, **kwargs):
    from furious.async import Async
    from furious import context
    ctx = context.Context(callbacks={'internal_vertex_combiner':l_combiner,
                                'leaf_combiner':l_combiner,
                                'success':example_callback_success})
    # Use the shorthand style, note that add returns the Async object.
    for i in xrange(2):
        ctx.add(target=example_function, args=[i])
        logging.info('Added job %d to context.', i)

    return ctx
