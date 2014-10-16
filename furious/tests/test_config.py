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
import unittest
import os
from mock import patch


class TestConfigurationLoading(unittest.TestCase):
    def setUp(self):
        super(TestConfigurationLoading, self).setUp()
        self._reset_config()

    def tearDown(self):
        super(TestConfigurationLoading, self).tearDown()
        self._reset_config()

    def _reset_config(self):
        """Reset config."""
        from furious.config import get_config
        from furious.config import default_config

        config = get_config()

        get_config().clear()
        config.update(default_config())

    def test_completion_config(self):

        from furious.config import get_completion_cleanup_delay
        from furious.config import get_completion_cleanup_queue
        from furious.config import get_completion_default_queue

        expected = 'default'
        queue = get_completion_cleanup_queue()
        self.assertEqual(queue, expected)

        queue = get_completion_default_queue()
        self.assertEqual(queue, expected)

        expected = 7600
        delay = get_completion_cleanup_delay()
        self.assertEqual(delay, expected)

    def test_load_yaml_config(self):
        """Ensure _load_yaml_config will load a specified path."""
        from furious.config import _load_yaml_config

        contents = _load_yaml_config(os.path.join('furious', '_furious.yaml'))

        e = 'persistence: ndb\ncleanupqueue: low-priority\ncleanupdelay: 7600\ndefaultqueue: default\n'

        self.assertEqual(contents, e)

    @patch('os.path.exists', autospec=True)
    def test_not_find_yaml(self, mock_exists):
        """Ensure when no furious.yaml exists, no file is found."""
        mock_exists.return_value = False

        from furious.config import find_furious_yaml

        config_yaml_path = find_furious_yaml()

        self.assertIsNone(config_yaml_path)

    def test_get_config(self):
        """Ensure a config contents produces the expected dictionary."""
        from furious.config import _parse_yaml_config

        example_yaml = str('secret_key: "blah"\n'
                           'persistence: bubble\n'
                           'task_system: flah\n')

        my_config = _parse_yaml_config(example_yaml)

        self.assertEqual(my_config, {'secret_key': 'blah',
                                     'persistence': 'bubble',
                                     'task_system': 'flah',
                                     'cleanupqueue': 'default',
                                     'cleanupdelay': 7600,
                                     'defaultqueue': 'default'})

    def test_get_configured_persistence_exists(self):
        """Ensure a chosen persistence module is selected."""
        from furious.config import _parse_yaml_config

        example_yaml = str('secret_key: "blah"\n'
                           'persistence: bubble\n'
                           'task_system: flah\n')

        my_config = _parse_yaml_config(example_yaml)

        from furious import config

        config._config = my_config

        persistence_module = config.get_default_persistence_engine(
            {'bubble': 'furious.config'})

        self.assertEqual(persistence_module, config)

    def test_get_config_invalid_yaml(self):
        """Ensure an invalid yaml file will raise InvalidYamlFile."""
        from furious.config import InvalidYamlFile
        from furious.config import _parse_yaml_config

        example_yaml = str('secret_key:"blah"\n'
                           'persistence:bubble\n'
                           'task_system:flah\n')

        self.assertRaises(InvalidYamlFile, _parse_yaml_config, example_yaml)

    def test_get_configured_module_by_path(self):
        """Ensure _get_configured_module loads options by path."""
        from furious.config import _get_configured_module
        from furious import config

        config.get_config()['test_option'] = 'furious.config'

        module = _get_configured_module('test_option')

        self.assertEqual(module, config)

    def test_get_configured_module_by_name(self):
        """Ensure _get_configured_module loads options by name."""
        from furious.config import _get_configured_module
        from furious import async
        from furious import config

        known_modules = {'cfg': 'furious.async'}

        config.get_config()['other_option'] = 'cfg'

        module = _get_configured_module('other_option', known_modules)

        self.assertEqual(module, async)

    def test_get_config_empty_yaml(self):
        """Ensure an empty furious.yaml will produce a default config."""
        from furious.config import default_config
        from furious.config import _parse_yaml_config

        example_yaml = str('')

        my_config = _parse_yaml_config(example_yaml)

        self.assertEqual(my_config, default_config())
