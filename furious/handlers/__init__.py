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

import json

import logging

from ..async import Async
from .. import context
from ..processors import run_job

from ..context import _local

def process_async_task(headers, request_body):
    """Process an Async task and execute the requested function."""
    async_options = json.loads(request_body)
    async = Async.from_dict(async_options)

    logging.info(async._function_path)

    with context.execution_context_from_async(async):
        local_context = _local.get_local_context()
        executing_asyncs = local_context._executing_async
        logging.debug("local context initialized: {0}".format(local_context._initialized))
        logging.debug("executing asyncs: {0}".format(executing_asyncs))
        run_job()
        executing_asyncs = local_context._executing_async
        logging.debug("local context initialized: {0}".format(local_context._initialized))
        logging.debug("executing asyncs after run_job: {0}".format(executing_asyncs))

    return 200, async._function_path



