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

import webapp2
from webapp2 import Route

from furious.marker_tree.marker import Marker

JOB_ENDPOINT = '/_context_job'


def get_status(idx):
    """Helper function that gets the status of a job
    Args:
        idx: the id of a Context job
    Returns:
        a dictionary indicating the status of the job
    """
    status = {
        'progress': 0,
        'aborted': False,
        'complete': False,
        'warnings': False,
        'errors': False
    }

    root_marker = Marker.get(idx)

    status.update({'complete': root_marker.done})

    return status


class DoneCheckHandler(webapp2.RequestHandler):
    def get(self, idx):
        #subclass and add authorization
        #TODO: use config to allow user to specify auth function
        self.process(idx)

    def process(self, idx):
        """
        returns a value if the job is done
        """
        status = get_status(idx)

        self.response.content_type = "text/json"
        self.response.write(json.dumps(status))


class ResultRetriever(webapp2.RequestHandler):
    def get(self, idx):
        #subclass and add authorization
        # TODO: use hmac signed idx token for authorization
        #TODO: use config to allow user to specify auth function
        self.process(idx)

    def process(self, idx):
        """
        returns the result of the job
        """
        root_marker = Marker.get(idx)
        result = root_marker.result_to_dict()

        self.response.content_type = "text/json"
        self.response.write(json.dumps(result))


app = webapp2.WSGIApplication([
    Route('{0}/<idx:[^/]+>/done'.format(JOB_ENDPOINT), handler=DoneCheckHandler),
    Route('{0}/<idx:[^/]+>/result'.format(JOB_ENDPOINT), handler=ResultRetriever),
])
