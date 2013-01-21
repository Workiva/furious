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

from .async_intro import AsyncIntroHandler
from .context_intro import ContextIntroHandler
from .context_complex import ContextComplexHandler
from .callback import AsyncCallbackHandler
from .callback import AsyncErrorCallbackHandler
from .callback import AsyncAsyncCallbackHandler
from .simple_workflow import SimpleWorkflowHandler
from .complex_workflow import ComplexWorkflowHandler


app = webapp2.WSGIApplication([
    ('/', AsyncIntroHandler),
    ('/context', ContextIntroHandler),
    ('/context/complex', ContextComplexHandler),
    ('/callback', AsyncCallbackHandler),
    ('/callback/error', AsyncErrorCallbackHandler),
    ('/callback/async', AsyncAsyncCallbackHandler),
    ('/workflow', SimpleWorkflowHandler),
    ('/workflow/complex', ComplexWorkflowHandler),
])
