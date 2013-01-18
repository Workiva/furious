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

"""A very basic async example.

This will insert an async task to run the example_function.
"""

import logging

import webapp2


class AsyncIntroHandler(webapp2.RequestHandler):
    """Demonstrate the creation and insertion of a single furious task."""
    def get(self):
        from furious.async import Async

        # Instantiate an Async object.
        async_task = Async(
            target=example_function, args=[1], kwargs={'some': 'value'})

        # Insert the task to run the Async object, note that it may begin
        # executing immediately or with some delay.
        async_task.start()

        logging.info('Async job kicked off.')

        self.response.out.write('Successfully inserted Async job.')


def example_function(*args, **kwargs):
    """This function is called by furious tasks to demonstrate usage."""
    logging.info('example_function executed with args: %r, kwargs: %r',
                 args, kwargs)

    return args
