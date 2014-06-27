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

import logging
import time

import webapp2

from furious import context


class LimitHandler(webapp2.RequestHandler):

    def get(self):
        sleep = self.request.get('sleep', 1)
        num = int(self.request.get('num', 1))
        queue = self.request.get('queue', 'default')

        logging.info('sleep: %s', sleep)
        logging.info('num: %s', num)
        logging.info('queue: %s', queue)

        with context.new() as ctx:
            for i in xrange(int(num)):
                ctx.add(sleeper, (sleep,), queue=queue)

        self.response.out.write('Successfully inserted a group of Async jobs.')


def sleeper(sleep):
    logging.info('sleeper before')
    logging.info('sleep: %s', sleep)
    time.sleep(float(sleep))
    logging.info('sleeper after')

