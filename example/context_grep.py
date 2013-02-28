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

import logging
import os
import re
import json

import webapp2
from webapp2_extras import jinja2

from furious.extras.callbacks import small_aggregated_results_success_callback
from furious.extras.combiners import lines_combiner
from furious.handlers.context_job import JOB_ENDPOINT


class DirectoryWalker:
    """From http://effbot.org/librarybook/os-path-walk-example-3.py"""
    # a forward iterator that traverses a directory tree

    def __init__(self, directory):
        self.stack = [directory]
        self.files = []
        self.index = 0

    def __getitem__(self, index):
        while 1:
            try:
                a_file = self.files[self.index]
                self.index += 1
            except IndexError:
                # pop next directory from stack
                self.directory = self.stack.pop()
                self.files = os.listdir(self.directory)
                self.index = 0
            else:
                # got a filename
                fullname = os.path.join(self.directory, a_file)
                if os.path.isdir(fullname) and not os.path.islink(fullname):
                    self.stack.append(fullname)
                return fullname


class ContextGrepHandler(webapp2.RequestHandler):
    def get(self, grouper=False):
        """
        This request handler will take a query from the query string
        of the url and launch an asynchronous process that
        looks through each file in this app directory finding each line
        that has the query string.
        The handler will immediately return the job id.
        When the job is done, it will set a value in memcache with the key
        Furious:the job id.
        once that's set, the result can be loaded with
        result = ndb.Key('Result',the job id).get()
        if result:
            return result.result

        """

        query = self.request.get('query')
        curdir = os.getcwd()
        if grouper:
            ctx = context_grepp_grouper(query, curdir)
        else:
            ctx = context_grepp(query, curdir)
        self.response.content_type = "text/json"
        self.response.out.write(json.dumps({
            'success': True,
            'job_id': ctx.id,
            'check_done_url': "{0}/{1}/done".format(JOB_ENDPOINT, ctx.id),
            'result_url': "{0}/{1}/result".format(JOB_ENDPOINT, ctx.id),
            'query': query,
        }))


class GrepViewHandler(webapp2.RequestHandler):
    """Grep view to search this code base and see the results

    url: host/context/grep
    """

    @webapp2.cached_property
    def jinja2(self):
        # Returns a Jinja2 renderer cached in the app registry.
        return jinja2.get_jinja2(app=self.app)

    def render_response(self, _template, **context):
        # Renders a template and writes the result to the response.
        rv = self.jinja2.render_template(_template, **context)
        self.response.write(rv)

    def get(self):
        context = {}

        self.render_response('grep_codebase.html', **context)


def simultaneous_grepp(query, directory):
    """
    args:
        query: a string
        directory: a directory path
    returns:
        A Context instance: one Async task for each
        item in the directory
        if the item is a file it adds an
        Async that will run grep_file on it

        if the item is a directory it adds an
        Async that will run simultaneous_grepp
        which, when processed will start the same asynchronous process
        on that directory
    """
    from furious import context

    dir_contents = os.listdir(directory)
    ctx = context.Context(callbacks={
        'internal_vertex_combiner': lines_combiner,
        'leaf_combiner': lines_combiner,
        'success': small_aggregated_results_success_callback})

    for item in dir_contents:
        path = os.path.join(directory, item)
        if os.path.isdir(path):
            ctx.add(target=simultaneous_grepp, args=[query, path])
        else:
            if item.endswith('.py'):
                ctx.add(target=grep_file, args=[query, path],
                        callbacks={'success': log_results})

    if ctx._tasks:
        return ctx

    return None


def context_grepp(query, directory):
    """
    args:
        query: a string
        directory: a directory path
    returns:
        the job id

    this kicks off the whole process get the job context
    from simultaneous_grepp and then starting it.
    """
    ctx = simultaneous_grepp(query, directory)
    if ctx:
        ctx.start()
        return ctx
    return None


def context_grepp_grouper(query, directory):
    from furious import context

    ctx = context.Context(callbacks={
        'internal_vertex_combiner': lines_combiner,
        'leaf_combiner': lines_combiner,
        'success': small_aggregated_results_success_callback})

    for path in DirectoryWalker(directory):
        if path.endswith('.py'):
            ctx.add(target=grep_file, args=[query, path],
                    callbacks={'success': log_results})

    if ctx:
        ctx.start()
        return ctx
    return None


def grep_file(query, item):
    """
    args:
        query: a string
        item: a file path
    returns:
        a list containing a line for every line in the file
        containing the query
    """
    results = []
    logging.info("grepping file: {0}".format(item))
    for index, line in enumerate(open(item)):
        if re.search("{0}".format(query), line):
            results.append('{0}:{1} {2}'.format(item, index + 1, line))
    return results


def log_results():
    from furious.context import get_current_async

    async = get_current_async()
    for result in async.result:
        logging.info(result)
