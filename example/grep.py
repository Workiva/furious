import logging
import os
import os.path
import re

import webapp2

from furious.async import Async

class GrepHandler(webapp2.RequestHandler):
    def get(self):
        query = self.request.get('query')
        curdir = os.getcwd()
        build_and_start(query, curdir)
        self.response.out.write('starting grep for query: %s' % query)

def build_and_start(query, directory):
        async_task = Async(
            target=grep, args=[query, directory], callbacks={'success': all_done}
        )
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

def all_done():
    from furious.context import get_current_async

    async = get_current_async()

    for result in async.result:
        logging.info(result)

