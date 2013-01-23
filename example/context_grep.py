import logging
import os
import re

import webapp2
from example.context_complex import example_callback_success

class ContextGrepHandler(webapp2.RequestHandler):
    def get(self):
        query = self.request.get('query')
        curdir = os.getcwd()
        context_id = context_grepp(query, curdir)
        self.response.out.write('starting grep for query: '
                                '{0}, get results with {1}'.format(
            query,context_id))


def simultaneous_grepp(query,directory):
    from furious import context

    dir_contents = os.listdir(directory)
    ctx = context.Context(callbacks={'internal_vertex_combiner':lines_combiner,
                                'leaf_combiner':lines_combiner,
                                'success':example_callback_success})
    for item in dir_contents:
        path = os.path.join(directory, item)
        if os.path.isdir(path):
            ctx.add(target=simultaneous_grepp,args=[query, path])
        else:
            if item.endswith('.py'):
                ctx.add(target=grep_file,args=[query, path],
                    callbacks={'success': log_results})


    return ctx

def context_grepp(query,directory):
    ctx =  simultaneous_grepp(query,directory)
    ctx.start()
    return ctx.id


def lines_combiner(results):
    return reduce(lambda x,y: x+"".join(y) if isinstance(y,list)
            else x+y,results,"")


def grep_file(query, item):
#    logging.info("searching {0} in item {1}".format(query,item))
    results = []
    for index,line in enumerate(open(item)):
        if re.search("{0}".format(query),line):
            results.append('{0}:{1} {2}'.format(item,index+1,line))
#    if results:
#        logging.info("FOUND RESULTS!")
    return results

def log_results():
    from furious.context import get_current_async

    async = get_current_async()

    for result in async.result:
        logging.info(result)
