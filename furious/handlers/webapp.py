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

import webapp2

from . import process_async_task


class AsyncJobHandler(webapp2.RequestHandler):
    """Handles requests for the webapp framework."""
    def get(self):
        self._handle_task()

    def post(self):
        self._handle_task()

    def _handle_task(self):
        """Pass request info to the async framework."""
        headers = self.request.headers

        status_code, output = process_async_task(headers, self.request.body)

        self.response.set_status(status_code)
        self.response.out.write(output)

app = webapp2.WSGIApplication([
    ('.*', AsyncJobHandler)
])

