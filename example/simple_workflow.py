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

"""A Simple Workflow Example.

Inserts a Async targetting the simple_state_machine function. This function
does a random number generation. If that number falls above a certain range
it will return another Aysnc pointed at the same simple_state_machine function.
If the simple_state_machine random falls below the range it will return and
complete the simple workflow.
"""

import logging

import webapp2


class SimpleWorkflowHandler(webapp2.RequestHandler):
    """Demonstrate constructing a simple state machine."""
    def get(self):
        from furious.async import Async

        # Instantiate an Async object to start the chain.
        Async(target=simple_state_machine).start()

        logging.info('Async chain kicked off.')

        self.response.out.write('Successfully inserted Async chain starter.')


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
