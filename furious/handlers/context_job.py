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
import json
from google.appengine.api import memcache
from google.appengine.ext import ndb

import webapp2
from webapp2 import Route

class DoneCheckHandler(webapp2.RequestHandler):
    def get(self, id):
        #subclass and add authorization
        self.process(id)

    def process(self, id):
        """
        returns a value if the job is done
        """
        #TODO: add less ephemeral storage fallback
        #how to handle if the done marker gets cleared from memcache
        #without this being too slow? ndb entity with auto-caching?
        status = memcache.get("Furious:{0}".format(id))
        result_retrieval_url = None
        if status:
            result_retrieval_url = "/_context_job/{0}/result".format(id)

        self.response.content_type = "text/json"
        self.response.write(json.dumps(result_retrieval_url))


class ResultRetriever(webapp2.RequestHandler):
    def get(self, id):
        #subclass and add authorization
        # TODO: use xsrf token for authorization
        self.process(id)

    def process(self, id):
        """
        returns the result of the job
        """

        #TODO: change out for persistence abstraction
        result = ndb.Key('Result',id).get()
        #TODO: actual results may be too large for one
        # call, so the results may be an iterator or a
        # list of keys to results pages of data that the client
        # or secondary async process can get as needed
        # TODO: so this needs to handle that result intelligently


        self.response.content_type = "text/json"
        self.response.write(json.dumps(result.to_dict()
        if result else result))

app = webapp2.WSGIApplication([
    Route('/_context_job/<id:[^/]+>/done', handler=DoneCheckHandler),
    Route('/_context_job/<id:[^/]+>/result', handler=ResultRetriever),
    ])
