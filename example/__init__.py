
import webapp2

class IntroHandler(webapp2.RequestHandler):
    def get(self):
        pass

app = webapp2.WSGIApplication([
    ('/', IntroHandler),
])

