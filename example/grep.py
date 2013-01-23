import logging
import os
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
        target=grep, args=[query, directory],
        callbacks={'success': log_results}
    )
    async_task.start()

def grep_file(query, item):
    return ['%s: %s' % (item, line) for line in open(item)
            if re.search(query, line)]

def grep(query, directory):
    dir_contents = os.listdir(directory)
    logging.info("awesome super grepping for {0}".format(query))
    results = []
    for item in dir_contents:
        path = os.path.join(directory, item)
        logging.info("path is {0}".format(path))
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
