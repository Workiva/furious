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

"""This is a Furious example that greps the source code of the application and
logs a message with each line that satisifes the input regular expression."""

import logging
import os
import re

import webapp2

from furious import context
from furious.async import Async
from furious.async import defaults


class GrepHandler(webapp2.RequestHandler):
    """This is the handler that starts off a grep run. It takes the query out
    of the query string and starts a new Furious Context to run all of the
    Asyncs in."""

    def get(self):
        query = self.request.get('query')
        curdir = os.getcwd()

        # create the context and start the first Async
        with context.new():
            build_and_start(query, curdir)

        self.response.out.write('starting grep for query: %s' % query)


def log_results():
    """This is the callback that is run once the Async task is finished. It
    takes the output from grep and logs it."""
    from furious.context import get_current_async

    # Get the recently finished Async object.
    async = get_current_async()

    # Pull out the result data and log it.
    for result in async.result:
        logging.info(result)


def build_and_start(query, directory):
    """This function will create and then start a new Async task with the
    default callbacks argument defined in the decorator."""

    Async(target=grep, args=[query, directory]).start()


def grep_file(query, item):
    """This function performs the actual grep on a given file."""
    return ['%s: %s' % (item, line) for line in open(item)
            if re.search(query, line)]


@defaults(callbacks={'success': log_results})
def grep(query, directory):
    """This function will search through the directory structure of the
    application and for each directory it finds it launches an Async task to
    run itself. For each .py file it finds, it actually greps the file and then
    returns the found output."""

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

