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

import logging
import os
import re

import webapp2

from furious import context
from furious import defaults
from furious.async import Async


class GrepHandler(webapp2.RequestHandler):
    def get(self):
        query = self.request.get('query')
        curdir = os.getcwd()

        with context.new():
            build_and_start(query, curdir)
        self.response.out.write('starting grep for query: %s' % query)


@defaults(callbacks={'success': log_results})
def build_and_start(query, directory):
        async_task = Async(target=grep, args=[query, directory])
        async_task.start()


def grep_file(query, item):
    return ['%s: %s' % (item, line) for line in open(item)
            if re.search(query, line)]


def grep(query, directory):
    dir_contents = os.listdir(directory)
    results = []
    for item in dir_contents:
        path = os.path.join(directory, item)
        if os.path.isdir(path):
            build_and_start(query, path)
        else:
            if item.endswith('.py'):
                results.extend(grep_file(query, path))
    return results


def log_results():
    from furious.context import get_current_async

    async = get_current_async()

    for result in async.result:
        logging.info(result)
