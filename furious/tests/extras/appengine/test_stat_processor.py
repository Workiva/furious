import unittest

from mock import Mock

from furious.extras.appengine.stat_processor import StatProcessor


class StatProcessorTestCase(unittest.TestCase):

    def test_get_request_details_exist(self):
        """Ensure if the request details exist in the environment they are
        returned.
        """
        env = {
            'APPLICATION_ID': "app+id",
            'CURRENT_VERSION_ID': 'version_id',
            'CURRENT_MODULE_ID': 'module-id',
            'INSTANCE_ID': 'instance&id',
            'REQUEST_LOG_ID': 'request#id'
        }

        processor = StatProcessor(env, None)

        self.assertEqual(processor.get_request_details(), {
            'app_id': 'app_id',
            'version_id': 'version_id',
            'module_id': 'module_id',
            'instance_id': 'instance_id',
            'request_id': 'request#id'
        })

    def test_get_request_details_dont_exist(self):
        """Ensure if the request details exist in the environment they are
        returned.
        """
        processor = StatProcessor(None, None)
        processor.env = {}

        self.assertEqual(processor.get_request_details(), {
            'app_id': '',
            'version_id': '',
            'module_id': '',
            'instance_id': '',
            'request_id': ''
        })

    def test_get_host_is_server_name(self):
        """Ensure the SERVER_NAME is returned for the host if it exists."""
        processor = StatProcessor({'SERVER_NAME': 'sname'}, None)

        self.assertEqual(processor.get_host(), 'sname')

    def test_get_host_is_app_id(self):
        """Ensure the APPLICATION_ID is returned for the host if it exists and
        the SERVER_NAME does not.
        """
        processor = StatProcessor(None, None)
        processor.env = {'APPLICATION_ID': 'appid'}

        self.assertEqual(processor.get_host(), 'appid')

    def test_get_host_doesnt_exist(self):
        """Ensure the NO_APPID is returned if SERVER_NAME and APPLICATION_ID
        don't exist in the environment.
        """
        processor = StatProcessor(None, None)
        processor.env = {}

        self.assertEqual(processor.get_host(), 'NO_APPID')

    def test_get_task_stats(self):
        """Ensure the task stats are returned correctly."""
        env = {
            'HTTP_X_APPENGINE_TASKETA': 10,
            'HTTP_X_APPENGINE_TASKRETRYCOUNT': 2,
            'HTTP_X_APPENGINE_TASKEXECUTIONCOUNT': 3,
        }

        recorder = Mock()
        recorder.start_timestamp = 100

        processor = StatProcessor(env, recorder)

        self.assertEqual(processor.get_task_stats(), {
            'execution_count': 3,
            'gae_latency_seconds': 90.0,
            'retry_count': 2,
            'task_eta': 10.0
        })
