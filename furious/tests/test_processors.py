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
        from furious.errors import NotInContextError
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

        self.assertIsInstance(work.result.payload, AsyncException)

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

    @patch('__builtin__.dir')
    def test_AbortAndRestart(self, dir_mock):
        """Ensures when AbortAndRestart is raised the Async restarts."""
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.errors import AbortAndRestart
        from furious.processors import run_job

        dir_mock.side_effect = AbortAndRestart
        mock_success = Mock()
        mock_error = Mock()

        work = Async(target='dir',
                     callbacks={'success': mock_success,
                                'error': mock_error})

        with _ExecutionContext(work):
            self.assertRaises(AbortAndRestart, run_job)

        self.assertFalse(mock_success.called)
        self.assertFalse(mock_error.called)

    @patch('furious.async.Async.start', autospec=True)
    @patch('__builtin__.dir')
    def test_Abort(self, dir_mock, mock_start):
        """Ensures that when Abort is raised, the Async immediately stops."""
        import logging

        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.errors import Abort
        from furious.processors import run_job

        class AbortLogHandler(logging.Handler):

            def emit(self, record):
                if record.levelno >= logging.ERROR:
                    raise Exception('An Error level log should not be output')

        logging.getLogger().addHandler(AbortLogHandler())

        dir_mock.side_effect = Abort

        mock_success = Mock()
        mock_error = Mock()

        work = Async(target='dir',
                     callbacks={'success': mock_success,
                                'error': mock_error})

        with _ExecutionContext(work):
            run_job()

        self.assertFalse(mock_success.called)
        self.assertFalse(mock_error.called)
        self.assertFalse(mock_start.called)

        logging.getLogger().removeHandler(AbortLogHandler())


class TestHandleResults(unittest.TestCase):
    """Test that _handle_results does the Right Things."""

    def setUp(self):
        import os
        import uuid

        # Ensure each test looks like it is in a new request.
        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    @patch('furious.processors._process_results')
    def test_defaults_to_process_results(self, processor_mock):
        """Ensure _handle_results calls _process_results if not given."""
        from furious.processors import _handle_results

        _handle_results({})

        processor_mock.assert_called_once_with()

    def test_runs_given_function(self):
        """Ensure _handle_results calls the given results processor."""
        from furious.processors import _handle_results

        processor = Mock()

        _handle_results({'_process_results': processor})

        processor.assert_called_once_with()

    def test_runs_returned_async(self):
        """Ensure _handle_results runs Async returned by results processor."""
        from furious.async import Async
        from furious.processors import _handle_results

        processor = Mock()
        processor.return_value = Mock(spec=Async)

        _handle_results({'_process_results': processor})

        processor.return_value.start.assert_called_once_with()

    def test_starts_returned_context(self):
        """Ensure _handle_results starts Context returned by results processor.
        """
        from furious.context.context import Context
        from furious.processors import _handle_results

        processor = Mock()
        processor.return_value = Mock(spec=Context)

        _handle_results({'_process_results': processor})

        processor.return_value.start.assert_called_once_with()


class TestContextCompletionChecker(unittest.TestCase):
    """Test that _handle_context_completion_check does the Right Things."""

    def setUp(self):
        import os
        import uuid

        # Ensure each test looks like it is in a new request.
        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    def test_no_comletion(self):
        """Ensure does not fail if there's no completion checker."""
        from furious.async import Async
        from furious.processors import _handle_context_completion_check

        async = Async(dir)

        _handle_context_completion_check(async)

    def test_checker_called_with_async(self):
        """Ensure checker called with id as argument."""
        from furious.async import Async
        from furious.processors import _handle_context_completion_check

        async = Async(dir)
        checker = Mock()
        async.update_options(_context_checker=checker)

        _handle_context_completion_check(async)

        checker.assert_called_once_with(async)

    @unittest.skip('not yet implemented.')
    def test_async_checker_called_with_id(self):
        """Ensure an Async checker called with id as argument."""
        from furious.async import Async
        from furious.processors import _handle_context_completion_check

        async = Async(dir)
        checker = Mock(spec=Async)
        async.update_options(_context_checker=checker)

        _handle_context_completion_check(async)

        checker.update_options.assert_called_once_with(args=['someid'])
        checker.start.assert_called_once_with()

        # Make sure didn't try to "call" the Async.
        self.assertEqual(checker.call_count, 0)


@patch('furious.processors.get_current_async')
class ProcessResultsTestCase(unittest.TestCase):

    def test_is_success_with_callback(self, get_current_async):
        """Ensure a sucessful process executes the sucess callback."""
        from furious.processors import _process_results

        async = Mock()
        success_callback = Mock()

        async.get_callbacks.return_value = {
            'success': success_callback
        }
        get_current_async.return_value = async

        result = _process_results()

        self.assertEqual(result, success_callback.return_value)

    def test_is_success_with_async_callback(self, get_current_async):
        """Ensure a sucessful process executes the sucess callback and returns
        an async if it's callback is an async.
        """
        from furious.async import Async
        from furious.processors import _process_results

        async = Mock(spec=Async)
        success_callback = Mock(spec=Async)

        async.get_callbacks.return_value = {
            'success': success_callback
        }
        get_current_async.return_value = async

        result = _process_results()

        self.assertEqual(result, success_callback.start.return_value)

    def test_is_success_with_no_callback(self, get_current_async):
        """Ensure a sucessful process with no success callback returns the
        async result payload if one exists.
        """
        from furious.async import Async
        from furious.processors import _process_results

        async = Mock(spec=Async)

        async.get_callbacks.return_value = {}
        get_current_async.return_value = async

        result = _process_results()

        self.assertEqual(result, async.result.payload)

    def test_is_error_with_callback(self, get_current_async):
        """Ensure an error process executes the error callback."""
        from furious.async import Async
        from furious.processors import AsyncException
        from furious.processors import _process_results

        async = Mock(spec=Async)
        async.result.payload = AsyncException("", "", "", "")
        error_callback = Mock(spec=Async)

        async.get_callbacks.return_value = {
            'error': error_callback
        }
        get_current_async.return_value = async

        result = _process_results()

        self.assertEqual(result, error_callback.start.return_value)

    def test_is_error_with_no_callback(self, get_current_async):
        """Ensure an error process with no callback raises the error."""
        from furious.async import Async
        from furious.async import AsyncResult
        from furious.processors import encode_exception
        from furious.processors import _process_results

        async = Mock(spec=Async)

        try:
            raise Exception()
        except Exception, e:
            async.result = AsyncResult(payload=encode_exception(e),
                                       status=AsyncResult.ERROR)

        async.get_callbacks.return_value = {}
        get_current_async.return_value = async

        self.assertRaises(Exception, _process_results)


def _fake_async_returning_target(async_to_return):
    return async_to_return


def _fake_result_returning_callback():
    from furious.context import get_current_async

    return get_current_async().result.payload

