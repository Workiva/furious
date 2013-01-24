import logging
from google.appengine.api import memcache
from google.appengine.ext import ndb
import os
import re
import time

import webapp2
import json
from webapp2_extras import jinja2
from furious.extras.appengine.ndb import Result

class ContextGrepHandler(webapp2.RequestHandler):
    def get(self):
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
        job_id = context_grepp(query, curdir)
        self.response.content_type = "text/json"
        self.response.out.write(json.dumps({
            'success':True,
            'job_id':job_id,
            'query':query,
        }))

        #set start time for whole job

        memcache.set("JOBTIMESTART:{0}".format(job_id),str(time.time()))

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



class DoneCheckHandler(webapp2.RequestHandler):
    def get(self):
        """
        returns a value if the job is done
        """
        job_id = self.request.get('job_id')
        done = False
        if job_id:
            status = memcache.get("Furious:{0}".format(job_id))
            if status:
                done = True

        self.response.content_type = "text/json"
        self.response.write(json.dumps(done))

class ResultRetriever(webapp2.RequestHandler):
    def get(self):
        """
        returns the result of the job
        """
        job_id = self.request.get('job_id')
        result = False
        if job_id:
            result = ndb.Key('Result',job_id).get()

        self.response.content_type = "text/json"
        self.response.write(json.dumps(result.to_dict() if result else result))


def simultaneous_grepp(query,directory):
    """
    args:
        query: a string
        directory: a directory path
    returns:
        A Context instance one Async task for each
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
    if not ctx._tasks:
        #if a directory is empty, add a dummy task
        #so the whole job can know it's been completed
        ctx.add(target=no_files)


    return ctx

def context_grepp(query,directory):
    """
    args:
        query: a string
        directory: a directory path
    returns:
        the job id

    this kicks off the whole process get the job context
    from simultaneous_grepp and then starting it.
    """
    ctx =  simultaneous_grepp(query,directory)
    ctx.start()
    return ctx.id

def example_callback_success(id,result):
    """
    args:
        id: the job id
        result: the combined output of all the Async functions

    A Context success callback is passed the id of the job
    and the result of the job(the combined output of all the functions).
    In this case it checks the real time the job took
    At this point, the result can be no larger then 1MB
    """
    start_time = memcache.get("JOBTIMESTART:{0}".format(id))
    job_time = 0
    if start_time:
        try:
            start_time_f = float(start_time)
            job_time = time.time()-start_time_f
            logging.info("Job {0} took {1}".format(id,job_time))
        except ValueError:
            pass

    #write the result to the datastore
    result = Result(
        id=id,
        job_time=job_time,
        result=result)
    result.put()
    #Flag the job as done
    memcache.set("Furious:{0}".format(id), "done by callback")

def lines_combiner(results):
    """
    args:
        results
    returns:
        joins all the results together into a string
    """
    return reduce(lambda x,y: x+"".join(y) if isinstance(y,list)
            else x+y,results,"")

def no_files():
    """
    if a directory is empty, this will let the whole job complete
    """
    return ""

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
    for index,line in enumerate(open(item)):
        if re.search("{0}".format(query),line):
            results.append('{0}:{1} {2}'.format(item,index+1,line))
    return results

def log_results():
    from furious.context import get_current_async
    async = get_current_async()
    for result in async.result:
        logging.info(result)
