import unittest

from mock import patch

from furious.async import Async
from furious.context.context import Context

from furious.complete import initialize_completion
from furious.complete import execute_completion_callbacks
from furious.complete import handle_completion_start

from furious.job_utils import encode_callbacks


def simple_test():

    return 1


@patch('furious.complete.get_current_context')
class TestHandleCompletionStartTestCase(unittest.TestCase):

    @patch('furious.complete.setup_completion_callback')
    @patch('furious.complete.add_context_work')
    def test_handle_completion_start(self, work, setup, current):

        async = Async(target=simple_test)
        success_async = Async(target=simple_test)
        async.on_success = success_async
        current.return_value = None

        handle_completion_start(async)

        self.assertTrue(current.called)
        self.assertFalse(work.called)

    @patch('furious.complete.setup_completion_callback')
    @patch('furious.complete.add_context_work')
    def test_handle_completion_inprocess(self, work, setup, current):

        async = Async(target=simple_test)
        success_async = Async(target=simple_test)
        async.on_success = success_async
        current_context = Context()
        current_context.completion_id = '12344-4'
        current.return_value = current_context

        handle_completion_start(async)

        self.assertTrue(current.called)
        self.assertTrue(work.called)

    @patch('furious.complete.setup_completion_callback')
    @patch('furious.complete.add_context_work')
    def test_handle_completion_no_completionid(self, work, setup, current):

        async = Async(target=simple_test)
        success_async = Async(target=simple_test)
        async.on_success = success_async
        current_context = Context()
        current.return_value = current_context

        handle_completion_start(async)

        self.assertTrue(current.called)
        self.assertTrue(setup.called)
        self.assertFalse(work.called)

class TestCompleteMethodsTestCase(unittest.TestCase):

    def test_async_on_success(self):

        async = Async(target=simple_test)
        success_async = Async(target=simple_test)
        async.on_success = success_async
        result = async.on_success

        self.assertEqual(result, success_async)

    def test_async_on_failure(self):

        async = Async(target=simple_test)
        failure_async = Async(target=simple_test)
        async.on_failure = failure_async
        result = async.on_failure

        self.assertEqual(result, failure_async)

    def test_async_process_result(self):

        async = Async(target=simple_test)
        process = 'furious.tests.test_complete.simple_test'
        async.process_results = process

        expected = 1
        result = async.process_results
        self.assertEqual(expected, result())


@patch('furious.complete.add_work_to_work_id')
@patch('furious.complete.setup_completion_callback')
class TestFuriousCompleteStartTestCase(unittest.TestCase):

    def test_initialize_completeion_async(self, setup, add):

        completion_id = "12313-3"
        setup.return_value = completion_id

        async = Async(target=simple_test)

        initialize_completion(async)

        self.assertTrue(setup.called)
        self.assertEqual(async.completion_id, completion_id)
        self.assertFalse(add.called)

    def test_initialize_completion_context(self, setup, add):

        completion_id = "12313-3"
        setup.return_value = completion_id
        success_async = Async(target=simple_test)
        failure_context = Context()

        one_failure_async = Async(target=simple_test)
        two_failure_async = Async(target=simple_test)
        failure_context.add(one_failure_async)
        failure_context.add(two_failure_async)

        context = Context()
        context.on_success = success_async
        context.on_failure = failure_context
        async = Async(target=simple_test)

        context.add(async)
        initialize_completion(context)

        self.assertTrue(setup.called)
        self.assertEqual(async.completion_id, completion_id)
        self.assertEqual(context.completion_id, completion_id)
        self.assertTrue(add.called)
        on_success_args = add.call_args[1].get('on_success_args')
        callbacks = on_success_args.get('callbacks')
        self.assertIsNotNone(callbacks)
        result = callbacks.get('on_success')
        self.assertEqual(result, success_async.to_dict())


class TestExecuteCompletionCallbacksTestCase(unittest.TestCase):

    def test_no_callback(self):

        callbacks = None

        result = execute_completion_callbacks(callbacks)
        self.assertEqual(result, callbacks)

    @patch('furious.async.Async.start')
    def test_callbacks_async(self, start):

        success_async = Async(target=simple_test)
        async = Async(target=simple_test)
        async.on_success = success_async
        async.on_failure = simple_test

        callbacks = async._options.get('callbacks')
        encoded_callbacks = encode_callbacks(callbacks)

        execute_completion_callbacks(encoded_callbacks)

        self.assertTrue(start.called)

    @patch('furious.async.Async.start')
    def test_callbacks_async_failure(self, start):

        success_async = Async(target=simple_test)
        async = Async(target=simple_test)
        async.on_success = success_async
        async.on_failure = simple_test

        callbacks = async._options.get('callbacks')
        encoded_callbacks = encode_callbacks(callbacks)

        execute_completion_callbacks(encoded_callbacks, True, "Exception")

        self.assertTrue(start.called)

    @patch('furious.context.context.Context.start')
    @patch('furious.async.Async.start')
    def test_callbacks_context(self, a_start, c_start):

        success_async = Async(target=simple_test)
        context = Context()
        context.on_success = success_async

        async = Async(target=simple_test)
        async.on_success = context
        async.on_failure = simple_test

        callbacks = async._options.get('callbacks')

        encoded_callbacks = encode_callbacks(callbacks)

        execute_completion_callbacks(encoded_callbacks)

        self.assertFalse(a_start.called)
        self.assertTrue(c_start.called)
