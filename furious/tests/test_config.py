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
    def test_load_yaml_config(self):
        """Ensure _load_yaml_config will load a specified path."""
        from furious.config import _load_yaml_config

        contents = _load_yaml_config(os.path.join('furious', '_furious.yaml'))

        self.assertEqual(contents, "persistence: ndb\n")

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
                                     'task_system': 'flah'})

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

    def test_get_persistence_module(self):
        """Ensure the chosen persistence module will load a module."""
        from furious.config import get_persistence_module
        from furious import config

        module = get_persistence_module('furious.config')

        self.assertEqual(module, config)

    def test_get_config_empty_yaml(self):
        """Ensure an empty furious.yaml will produce a default config."""
        from furious.config import default_config
        from furious.config import _parse_yaml_config

        example_yaml = str('')

        my_config = _parse_yaml_config(example_yaml)

        self.assertEqual(my_config, default_config())
