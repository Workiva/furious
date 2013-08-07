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

"""A very basic async example using a queue group.

This will insert 500 async tasks to run the example_function in a queue
group.
"""

import logging

import webapp2


class QueueGroupIntroHandler(webapp2.RequestHandler):
    """Demonstrate the creation and insertion of 500 furious tasks to be run
    in a queue group with random distribution."""
    def get(self):
        from furious import context

        with context.new() as ctx:
            for i in xrange(500):
                ctx.add(target=example_function, args=[i],
                        queue_group='workers', queue_count=4)

        logging.info('500 Async jobs inserted.')

        self.response.out.write('Successfully inserted 500 Async jobs.')


def example_function(*args, **kwargs):
    """This function is called by furious tasks to demonstrate usage."""
    logging.info('example_function executed with args: %r, kwargs: %r',
                 args, kwargs)
    return args
