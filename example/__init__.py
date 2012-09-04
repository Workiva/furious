
import webapp2

from furious.async import ASYNC_ENDPOINT
from furious.handlers.webapp import AsyncJobHandler

class IntroHandler(webapp2.RequestHandler):
    def get(self):
        pass

app = webapp2.WSGIApplication([
    ('/', IntroHandler),
    ('%s.*' % ASYNC_ENDPOINT, AsyncJobHandler)
])

