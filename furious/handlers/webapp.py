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

import webapp2

from furious.handlers import process_async_task
from furious.errors import AbortAndRestart
from furious.config import get_csrf_check

class AsyncJobHandler(webapp2.RequestHandler):
    """Handles requests for the webapp framework."""
    def get(self):
        self._handle_task()

    def post(self):
        self._handle_task()

    def _handle_task(self):
        """Pass request info to the async framework."""
        # Check for CSRF
        get_csrf_check()(self.request)

        headers = self.request.headers

        message = None
        try:
            status_code, output = process_async_task(
                headers, self.request.body)
        except AbortAndRestart as restart:
            # Async retry status code
            status_code = 549
            message = 'Retry Async Task'
            output = str(restart)

        self.response.set_status(status_code, message)
        self.response.out.write(output)

app = webapp2.WSGIApplication([
    ('.*', AsyncJobHandler)
])

def csrf_check(request):
    """
    Throws an HTTP 403 error if a CSRF attack is detected, same logic as the deferred module.

    https://cloud.google.com/appengine/docs/standard/python/refdocs/modules/google/appengine/ext/deferred/deferred
    """
    in_prod = (
        not request.environ.get("SERVER_SOFTWARE").startswith("Devel"))
    if in_prod and request.environ.get("REMOTE_ADDR") != "0.1.0.2":
      logging.error("Detected an attempted CSRF attack from {}. This request did "
                    "not originate from Task Queue.".format(request.environ.get("REMOTE_ADDR")))
      webapp2.abort(403)
