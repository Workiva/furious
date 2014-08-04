#
# Copyright 2013 WebFilings, LLC
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
"""This is a simple RPC recorder based on the App Stats recorder.  It is
designed to be registered with an App Stats recorder proxy, then called via
the RPC pre and post hooks.  It collects data on the duration of RPCs.
"""
import json
import logging
import time

from furious.config import get_module


class Recorder(object):
    """In-memory state for the current request.

    An instance is created soon after the request is received, and
    set as the Recorder for the current request in the
    RequestLocalRecorderProxy in the global variable 'recorder_proxy'.  It
    collects information about the request and about individual RPCs
    made during the request, until just before the response is sent out,
    when the recorded information is logged by calling the emit() method.
    """

    def __init__(self, env):
        """Constructor.

        Args:
        env: A dict giving the CGI or WSGI environment.
        """
        processor_cls = get_module("stat_processor")
        self.processor = processor_cls(env, self) if processor_cls else None

        self.start_timestamp = time.time()
        self.end_timestamp = self.start_timestamp
        self.env = env
        self.status_code = None

    def get_payload(self):
        """Create a dict consisting of the async full id, async function path
        as the url, request info, application id as the address and combine it
        with the passed in task info. Then json dumps that and return it.
        """

        try:
            payload = self._get_request_info() or {}
            payload['status_code'] = self.status_code
            payload['host'] = self._get_host()

            payload.update(self._get_async_details())

            task_stats = self._get_task_stats()
            if task_stats:
                payload.update(task_stats)

            payload = json.dumps(payload)
        except Exception, e:
            logging.info(e)
            return None

        return payload

    def _get_async_details(self):
        """Pull the async from the local context and return the id and function
        path in a dict if it exists.
        """
        from furious.context._local import get_local_context

        context = get_local_context()

        if not context or not context._executing_async_context:
            return {}

        async = context._executing_async_context.async

        if not async:
            return {}

        return {
            'id': async.full_id,
            'url': async.function_path
        }

    def _get_request_info(self):
        if self.processor:
            return self.processor.get_request_details()

        return {}

    def _get_host(self):
        if self.processor:
            return self.processor.get_host()

        return self.env.get('SERVER_NAME')

    def _get_task_stats(self):
        """Processes the header from task requests to log analytical data."""

        end = time.time()
        task_stats = {
            'ran': self.start_timestamp,
            'end': end,
            'run_time': end - self.start_timestamp
        }

        if self.processor:
            task_stats.update(self.processor.get_task_stats())

        return task_stats
