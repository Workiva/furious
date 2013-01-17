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


class ComplexWorkflowHandler(webapp2.RequestHandler):
    """Demonstrate constructing a more complex state machine."""
    def get(self):
        from furious.async import Async

        # Instantiate an Async object to start the machine in state alpha.
        Async(target=complex_state_generator_alpha).start()

        logging.info('Async chain kicked off.')

        self.response.out.write('Successfully inserted Async chain starter.')


@defaults(
    callbacks={'success': "example.complex_workflow.state_machine_success"})
def complex_state_generator_alpha(last_state=''):
    """Pick a state."""
    from random import choice

    states = ['ALPHA', 'ALPHA', 'ALPHA', 'BRAVO', 'BRAVO', 'DONE']
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


@defaults(
    callbacks={'success': "example.complex_workflow.state_machine_success"})
def complex_state_generator_bravo(last_state=''):
    """Pick a state."""
    from random import choice

    states = ['ALPHA', 'BRAVO', 'BRAVO', 'DONE']
    if last_state:
        states.remove(last_state)  # Slightly lower chances of previous state.

    state = choice(states)

    logging.info('Generating a state... %s', state)

    return state
