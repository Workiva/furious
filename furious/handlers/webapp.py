
import webapp2

from . import process_async_task


class AsyncJobHandler(webapp2.RequestHandler):
    """Handles requests for the webapp framework."""
    def get(self):
        self.handle_task()

    def post(self):
        self.handle_task()

    def handle_task(self):
        """Pass request info to the async framework."""
        headers = self.request.headers

        staus_code, output = process_async_task(headers, self.request.body)

        self.response.set_status(staus_code)
        self.response.out.write(output)

