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

"""A basic example to demonstrate the AbortAndRestart functionality."""

import logging

import webapp2


class AbortAndRestartHandler(webapp2.RequestHandler):
    """Demonstrate the AbortAndRestart functionality."""

    def get(self):
        from furious.async import Async

        # Instantiate an Async
        async_task = Async(target=aborting_function)

        # Start an Async()
        async_task.start()

        logging.info('Original Async kicked off.')

        self.response.write('Successfully inserted AbortAndRestart example.')


def aborting_function():
    """There is a 50% chance that this function will AbortAndRestart or
    complete successfully.

    The 50% chance simply represents a process that will fail half the time
    and succeed half the time.
    """
    import random

    logging.info('In aborting_function')

    if random.random() < .5:
        from furious.errors import AbortAndRestart

        logging.info('Getting ready to restart')

        # Raise AbortAndRestart like an Exception, and watch the magic happen.
        raise AbortAndRestart()

    logging.info('No longer restarting')

