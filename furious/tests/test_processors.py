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

from mock import Mock
from mock import patch


class TestRunJob(unittest.TestCase):
    """Test that run_job correctly executes functions from Async options."""

    def setUp(self):
        import os
        import uuid

        # Ensure each test looks like it is in a new request.
        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    @patch('__builtin__.dir')
    def test_runs_with_none_arg(self, dir_mock):
        """Ensure run_job calls with None arg."""
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.processors import run_job

        async = Async("dir", [None])

        with _ExecutionContext(async):
            run_job()

        dir_mock.assert_called_once_with(None)

    def test_dies_if_no_job(self):
        """Ensure run_job raises if there is no job in the Async."""
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.processors import run_job

        work = Async("dir", kwargs={'something': None})
        work._options.pop('job')
        assert 'job' not in work._options

        with _ExecutionContext(work):
            self.assertRaises(Exception, run_job)

    @patch('__builtin__.dir')
    def test_runs_with_none_kwarg(self, dir_mock):
        """Ensure run_job calls with a kwarg=None."""
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.processors import run_job

        work = Async("dir", kwargs={'something': None})

        with _ExecutionContext(work):
            run_job()

        dir_mock.assert_called_once_with(something=None)

    @patch('__builtin__.dir')
    def test_runs_with_non_arg_and_kwarg(self, dir_mock):
        """Ensure run_job calls with a None arg and kwarg=None."""
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.processors import run_job

        work = Async("dir", [None], {'something': None})

        with _ExecutionContext(work):
            run_job()

        dir_mock.assert_called_once_with(None, something=None)

    def test_raises_on_missing_job(self):
        """Ensure run_job raises an exception on bogus standard import."""
        from furious.async import Async
        from furious.context import NotInContextError
        from furious.processors import run_job

        work = Async("nothere")
        work._options.pop('job')
        assert 'job' not in work._options

        self.assertRaises(NotInContextError, run_job)

    def test_handles_job_exception(self):
        """Ensure run_job catches and encodes exceptions to the async result,
        then raises them.
        """
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.processors import run_job
        from furious.processors import AsyncException

        work = Async(target=dir, args=[1, 2])

        with _ExecutionContext(work):
            self.assertRaises(TypeError, run_job)

        self.assertIsInstance(work.result, AsyncException)

    def test_calls_success_callback(self):
        """Ensure run_job calls the success callback after a successful run."""
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.processors import run_job

        call_count = []

        def do_things():
            call_count.append(1)

        work = Async(target=dir, args=[1],
                     callbacks={'success': do_things})

        with _ExecutionContext(work):
            run_job()

        self.assertEqual(1, len(call_count))

    def test_calls_error_callback(self):
        """Ensure run_job catches any exceptions raised by the job, then calls
        the error callback.
        """
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.processors import run_job

        call_count = []
        handle_count = []

        def handle_success():
            call_count.append(1)

        def handle_errors():
            handle_count.append(1)

        work = Async(target=dir, args=[1, 2],
                     callbacks={'success': handle_success,
                                'error': handle_errors})

        with _ExecutionContext(work):
            run_job()

        self.assertEqual(1, len(handle_count),
                         "Error handler called wrong number of times.")
        self.assertEqual(0, len(call_count),
                         "Success handler unexpectedly called.")

    def test_calls_async_callbacks(self):
        """Ensure run_job will call start on an Async object callback."""
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.processors import run_job

        callback = Mock(spec=Async)

        work = Async(target=dir, args=[1], callbacks={'success': callback})

        with _ExecutionContext(work):
            run_job()

        callback.start.assert_called_once_with()

    def test_starts_returned_async(self):
        """Ensure run_job calls returned Async objects."""
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.processors import run_job

        returned_async = Mock(spec=Async)

        work = Async(target=_fake_async_returning_target,
                     args=[returned_async])

        with _ExecutionContext(work):
            run_job()

        returned_async.start.assert_called_once_with()

    def test_starts_callback_returned_async(self):
        """Ensure run_job calls returned Async objects."""
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.processors import run_job

        returned_async = Mock(spec=Async)

        work = Async(target=_fake_async_returning_target,
                     args=[returned_async],
                     callbacks={'success': _fake_result_returning_callback})

        with _ExecutionContext(work):
            run_job()

        returned_async.start.assert_called_once_with()

    @patch('furious.async.Async.start')
    @patch('__builtin__.dir')
    def test_AbortAndRestart(self, dir_mock, mock_start):
        """Ensures when AbortAndRestart is raised the Async restarts."""
        from furious.async import AbortAndRestart
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.processors import run_job

        dir_mock.side_effect = AbortAndRestart
        mock_success = Mock()
        mock_error = Mock()

        work = Async(target='dir',
                     callbacks={'success': mock_success,
                                'error': mock_error})

        with _ExecutionContext(work):
            run_job()

        mock_start.assert_called_once()
        self.assertFalse(mock_success.called)
        self.assertFalse(mock_error.called)


def _fake_async_returning_target(async_to_return):
    return async_to_return


def _fake_result_returning_callback():
    from furious.context import get_current_async

    return get_current_async().result

