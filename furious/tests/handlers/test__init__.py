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

import unittest

from mock import patch

@patch('logging.debug')
class TestLogTaskInfo(unittest.TestCase):
    """Ensure that _log_task_info works as expected."""

    def test_all_headers(self, debug_mock):
        """Ensure _log_task_info produces the correct logs."""
        from furious import handlers
        headers = {
            'X-Appengine-Queuename': 'red',
            'X-Appengine-Taskname': 'green',
            'X-Appengine-Taskretrycount': 'blue',
            'X-Appengine-Taskexecutioncount': 'yellow',
            'X-Appengine-Tasketa': 'purple'
        }

        handlers._log_task_info(headers)

        expected_logs = ('TASK-INFO: '
            '{"eta": "purple", "retry_count": "blue", "queue_name": "red",'
            ' "task_name": "green", "execution_count": "yellow"}')

        debug_mock.assert_called_with(expected_logs)

    def test_no_headers(self, debug_mock):
        """Ensure _log_task_info produces the correct logs."""
        from furious import handlers
        headers = {}

        handlers._log_task_info(headers)

        expected_logs = ('TASK-INFO: '
            '{"eta": "", "retry_count": "", "queue_name": "",'
            ' "task_name": "", "execution_count": ""}')

        debug_mock.assert_called_with(expected_logs)

    def test_ignore_extra_headers(self, debug_mock):
        """Ensure _log_task_info ignores extra items in the header"""
        from furious import handlers
        headers = {
            'something-something': 'please-ignore-me',
            'X-Appengine-Queuename': 'red',
            'X-Appengine-Taskname': 'green',
            'X-Appengine-Taskretrycount': 'blue',
            'X-Appengine-Taskexecutioncount': 'yellow',
            'X-Appengine-Tasketa': 'purple'
        }

        handlers._log_task_info(headers)

        expected_logs = ('TASK-INFO: '
            '{"eta": "purple", "retry_count": "blue", "queue_name": "red",'
            ' "task_name": "green", "execution_count": "yellow"}')

        debug_mock.assert_called_with(expected_logs)
