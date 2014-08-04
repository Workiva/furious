import re
import os


class StatProcessor(object):

    def __init__(self, env, recorder):
        """Initialize the stat procossor with the environment variables and
        recorder.
        """
        self.env = {}
        self.recorder = recorder

        self.env.update(env if env else os.environ)

    def get_request_details(self):
        """Pull the appengine details from the environement variables (which
        will also include the request args.
        """
        return {
            'app_id': self._get_env_info('APPLICATION_ID', ''),
            'version_id': self._get_env_info('CURRENT_VERSION_ID', ''),
            'module_id': self._get_env_info('CURRENT_MODULE_ID', ''),
            'instance_id': self._get_env_info('INSTANCE_ID', ''),
            'request_id': self.env.get('REQUEST_LOG_ID', ''),
        }

    def get_host(self):
        """Return the server name or application name from the environemnt."""
        return self.env.get('APPLICATION_ID', 'NO_APPID')

    def get_task_stats(self):
        """Processes the environment data from task requests to get some
        analytical data.
        """

        task_eta = float(self.env.get('HTTP_X_APPENGINE_TASKETA', 0.0))

        return {
            'retry_count': self.env.get('HTTP_X_APPENGINE_TASKRETRYCOUNT', 0),
            'execution_count': self.env.get(
                'HTTP_X_APPENGINE_TASKEXECUTIONCOUNT', 0),
            'task_eta': task_eta,
            'gae_latency_seconds': self.recorder.start_timestamp - task_eta,
        }

    def _get_env_info(self, env_id, default=''):
        """Return the string pulled from os.environ with special characters
        removed.
        """
        return _clean_string(self.env.get(env_id, default))


def _clean_string(string):
    """Return the string with special characters removed."""
    return re.sub('[^a-zA-Z0-9\n]', '_', string.strip()).strip('_')
