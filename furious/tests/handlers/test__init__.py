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


import unittest

from mock import patch


@patch('time.time')
@patch('logging.debug')
class TestLogTaskInfo(unittest.TestCase):
    """Ensure that _log_task_info works as expected."""

    def test_all_headers(self, debug_mock, time_mock):
        """Ensure _log_task_info produces the correct logs."""
        from furious import handlers
        time_mock.return_value = 1.5
        headers = {
            'X-Appengine-Taskretrycount': 'blue',
            'X-Appengine-Taskexecutioncount': 'yellow',
            'X-Appengine-Tasketa': '0.50'
        }

        handlers._log_task_info(headers)

        expected_logs = (
            '{"ran": 1.5, "retry_count": "blue", "gae_latency_seconds": 1.0, '
            '"task_eta": 0.5, "execution_count": "yellow"}')

        debug_mock.assert_called_with('TASK-INFO: %s', expected_logs)

    def test_no_headers(self, debug_mock, time_mock):
        """Ensure _log_task_info produces the correct logs."""
        from furious import handlers
        time_mock.return_value = 1.5
        headers = {}

        handlers._log_task_info(headers)

        expected_logs = (
            '{"ran": 1.5, "retry_count": "", "gae_latency_seconds": 1.5, '
            '"task_eta": 0.0, "execution_count": ""}')

        debug_mock.assert_called_with('TASK-INFO: %s', expected_logs)

    def test_ignore_extra_headers(self, debug_mock, time_mock):
        """Ensure _log_task_info ignores extra items in the header"""
        from furious import handlers
        time_mock.return_value = 1.5
        headers = {
            'something-something': 'please-ignore-me',
            'X-Appengine-Taskretrycount': 'blue',
            'X-Appengine-Taskexecutioncount': 'yellow',
            'X-Appengine-Tasketa': '0.50'
        }

        handlers._log_task_info(headers)

        expected_logs = (
            '{"ran": 1.5, "retry_count": "blue", "gae_latency_seconds": 1.0, '
            '"task_eta": 0.5, "execution_count": "yellow"}')

        debug_mock.assert_called_with('TASK-INFO: %s', expected_logs)
