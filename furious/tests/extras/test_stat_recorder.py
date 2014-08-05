import json
import unittest

from mock import Mock
from mock import patch

from furious.extras.stat_recorder import Recorder


@patch('furious.extras.stat_recorder.get_module')
class RecorderTestCase(unittest.TestCase):

    def test_recorder_has_processor(self, get_module):
        """Ensure the processor is set if it exists in the config."""
        env = {}
        processor = Mock()

        get_module.return_value = processor

        recorder = Recorder(env)

        get_module.assert_called_once_with("stat_processor")

        self.assertIsNotNone(recorder.start_timestamp)
        self.assertEqual(recorder.start_timestamp, recorder.end_timestamp)
        self.assertEqual(recorder.env, env)
        self.assertIsNone(recorder.status_code)
        self.assertEqual(recorder.processor, processor.return_value)

    def test_recorder_has_no_processor(self, get_module):
        """Ensure the processor is set to None if it doesn't exist in the
        config.
        """
        env = {}

        get_module.return_value = None

        recorder = Recorder(env)

        get_module.assert_called_once_with("stat_processor")

        self.assertIsNotNone(recorder.start_timestamp)
        self.assertEqual(recorder.start_timestamp, recorder.end_timestamp)
        self.assertEqual(recorder.env, env)
        self.assertIsNone(recorder.status_code)
        self.assertIsNone(recorder.processor)

    @patch('furious.extras.stat_recorder.time.time')
    def test_get_payload_with_stats_no_processor(self, get_time, get_module):
        """Ensure get payload returns the expeted results with task stats
        included.
        """
        get_time.side_effect = (1, 2)
        get_module.return_value = None

        recorder = Recorder({})

        payload = recorder.get_payload()

        self.assertEqual(json.loads(payload), json.loads(json.dumps({
            "ran": 1,
            "status_code": None,
            "host": None,
            "end": 2,
            "run_time": 1
        })))

    @patch('furious.extras.stat_recorder.time.time')
    def test_get_payload_with_stats_with_processor(self, get_time, get_module):
        """Ensure get payload returns the expeted results with task stats
        included.
        """
        get_time.side_effect = (1, 2)

        processor = Mock()
        get_module.return_value = processor

        processor.return_value.get_request_details.return_value = {
            'preq': 'foo'
        }

        processor.return_value.get_host.return_value = "phost"
        processor.return_value.get_task_stats.return_value = {
            'pstats': 'bar'
        }

        recorder = Recorder({})

        payload = recorder.get_payload()

        self.assertEqual(json.loads(payload), json.loads(json.dumps({
            "preq": "foo",
            "ran": 1,
            "status_code": None,
            "host": "phost",
            "end": 2,
            "run_time": 1,
            "pstats": "bar"
        })))

    def test_get_payload_with_stats_with_processor_raises(self, get_module):
        """Ensure get payload that raises returns None."""
        processor = Mock()
        processor.get_request_details.side_effect = ValueError
        get_module.return_value = processor

        recorder = Recorder({})

        payload = recorder.get_payload()

        self.assertIsNone(payload)

    @patch('furious.context._local.get_local_context')
    def test_get_async_details(self, get_context, get_module):
        """Ensure if the async is found that the id and function path are
        returned in a dict.
        """
        async = Mock()
        async.full_id = "fullid"
        async.function_path = "funcpath"

        context = Mock()
        context._executing_async_context.async = async
        get_context.return_value = context

        recorder = Recorder({})

        self.assertEqual(recorder._get_async_details(), {
            'id': 'fullid',
            'url': 'funcpath'
        })

    @patch('furious.context._local.get_local_context')
    def test_get_async_details_with_no_context(self, get_context, get_module):
        """Ensure if no context is found that an empty dict is returned."""
        get_context.return_value = None

        recorder = Recorder({})

        self.assertEqual(recorder._get_async_details(), {})

    @patch('furious.context._local.get_local_context')
    def test_get_async_details_with_no_executing_context(self, get_context,
                                                         get_module):
        """Ensure if no _executing_async_context is on the context that an
        empty dict is returned.
        """
        context = Mock()
        context._executing_async_context = None
        get_context.return_value = context

        recorder = Recorder({})

        self.assertEqual(recorder._get_async_details(), {})

    @patch('furious.context._local.get_local_context')
    def test_get_async_details_with_no_async(self, get_context, get_module):
        """Ensure if no async is on the _executing_async_context that an
        empty dict is returned.
        """
        context = Mock()
        context._executing_async_context.async = None
        get_context.return_value = context

        recorder = Recorder({})

        self.assertEqual(recorder._get_async_details(), {})

