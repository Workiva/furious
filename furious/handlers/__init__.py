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
import time

import logging

from furious.async import async_from_options
from furious import context
from furious.processors import run_job


def process_async_task(headers, request_body):
    """Process an Async task and execute the requested function."""
    async_options = json.loads(request_body)
    async = async_from_options(async_options)

    _log_task_info(headers,
                   extra_task_info=async.get_options().get('_extra_task_info'))

    logging.info(async._function_path)

    with context.execution_context_from_async(async):
        run_job()

    return 200, async._function_path


def _log_task_info(headers, extra_task_info=None):
    """Processes the header from task requests to log analytical data."""
    ran_at = time.time()
    task_eta = float(headers.get('X-Appengine-Tasketa', 0.0))
    task_info = {
        'retry_count': headers.get('X-Appengine-Taskretrycount', ''),
        'execution_count': headers.get('X-Appengine-Taskexecutioncount', ''),
        'task_eta': task_eta,
        'ran': ran_at,
        'gae_latency_seconds': ran_at - task_eta
    }

    if extra_task_info:
        task_info['extra'] = extra_task_info

    logging.debug('TASK-INFO: %s', json.dumps(task_info))
