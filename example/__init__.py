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

"""Contained within this module are several working examples showing basic
usage and complex context-based chaining.

The examples demonstrate basic task execution, and also the basics of creating
more complicated processing pipelines.
"""

import webapp2

from .abort_and_restart import AbortAndRestartHandler
from .async_intro import AsyncIntroHandler
from .batcher import BatcherHandler
from .batcher import BatcherStatsHandler
from .batcher import BatcherViewHandler
from .callback import AsyncCallbackHandler
from .callback import AsyncErrorCallbackHandler
from .callback import AsyncAsyncCallbackHandler
from .complex_workflow import ComplexWorkflowHandler
from .context_intro import ContextIntroHandler
from .context_events import ContextEventsHandler
from .context_completion_with_results import ContextCompletionHandler
from .grep import GrepHandler
from .simple_workflow import SimpleWorkflowHandler
from .limits import LimitHandler

config = {
    'webapp2_extras.jinja2': {
        'template_path': 'example/templates'
    }
}

app = webapp2.WSGIApplication([
    ('/', AsyncIntroHandler),
    ('/abort_and_restart', AbortAndRestartHandler),
    ('/context', ContextIntroHandler),
    ('/context/event', ContextEventsHandler),
    ('/context/completion', ContextCompletionHandler),
    ('/callback', AsyncCallbackHandler),
    ('/callback/error', AsyncErrorCallbackHandler),
    ('/callback/async', AsyncAsyncCallbackHandler),
    ('/workflow', SimpleWorkflowHandler),
    ('/workflow/complex', ComplexWorkflowHandler),
    ('/batcher', BatcherViewHandler),
    ('/batcher/run', BatcherHandler),
    ('/batcher/stats', BatcherStatsHandler),
    ('/grep', GrepHandler),
    ('/limits', LimitHandler),
], config=config)

