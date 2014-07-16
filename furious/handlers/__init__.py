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

from furious.async import async_from_options
from furious import context
from furious.errors import AbortAndRestart
from furious.processors import run_job


def handle_task(headers, request_body):
    """Handle an Async task, execute the requested function and log the
    details.
    """
    message = None

    init_stats = start_stats()

    try:
        status_code, async = process_async_task(
            headers, request_body)
        output = async.function_path
    except AbortAndRestart as restart:
        # Async retry status code
        status_code = 549
        message = 'Retry Async Task'
        output = str(restart)

    try:
        log_task_info(headers, async, status_code, init_stats)
    except:
        pass

    return status_code, message, output


def process_async_task(headers, request_body):
    """Process an Async task and execute the requested function."""
    async_options = json.loads(request_body)
    async = async_from_options(async_options)

    logging.info(async.function_path)

    with context.execution_context_from_async(async):
        run_job()

    return 200, async


def start_stats():
    import time

    return {
        'start_time': time.time()
    }


def log_task_info(headers, async, status_code, init_stats):
    from furious.config import get_default_logging_engine

    logger = get_default_logging_engine()

    if not logger:
        return

    logger.log(async, headers, status_code, init_stats)
