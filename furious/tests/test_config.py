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
import os
import unittest

from mock import MagicMock
from mock import Mock
from mock import patch


def mock_get_config_as(yaml_contents):
    """Mocks open to return yaml_contents instead of furious.yaml
    Args:
        yaml_contents: yaml as a string
    Returns:
        A config dictionary parsed from the yaml in
        yaml_contents instead of any actual furious.yaml file.
    """
    with patch('__builtin__.open', create=True) as mock_open:
        mock_open.return_value = MagicMock(spec=file)
        manager = mock_open.return_value.__enter__.return_value
        manager.read.return_value = yaml_contents
        from furious.config import _parse_yaml_config

        return _parse_yaml_config()


class TestConfigurationLoading(unittest.TestCase):
    def test_module_import_missing_module(self):
        from furious.config import BadModulePathError
        from furious.config import module_import

        self.assertRaises(BadModulePathError,
                          module_import, 'furious.extras.not_here')

    def test_module_import(self):
        from furious.config import module_import
        from .. import config

        module = module_import('furious.config')
        self.assertEqual(module, config)

    def test_not_find_yaml(self):
        os.path.exists = Mock(return_value=False)
        from furious.config import find_furious_yaml

        config_yaml_path = find_furious_yaml()
        self.assertIsNone(config_yaml_path)

    def test_get_config(self):
        os.path.exists = Mock(return_value=True)
        example_yaml = str('secret_key: "blah"\n'
                           'persistence: bubble\n'
                           'task_system: flah\n')

        my_config = mock_get_config_as(example_yaml)

        self.assertEqual(my_config, {'secret_key': 'blah',
                                     'persistence': 'bubble',
                                     'task_system': 'flah'})

    def test_get_configured_persistence_exists(self):
        os.path.exists = Mock(return_value=True)
        example_yaml = str('secret_key: "blah"\n'
                           'persistence: bubble\n'
                           'task_system: flah\n')

        my_config = mock_get_config_as(example_yaml)

        from .. import config

        config._config = my_config

        persistence_module = config.get_default_persistence_engine(
            {'bubble': 'furious.config'})

        self.assertEqual(persistence_module, config)

    def mock_get_config_raises(self, yaml_contents, exception=Exception):
        """Mocks open to return yaml_contents instead of furious.yaml
        and expects yaml_contents to be malformed in a way to
        cause exception to raise
        Args:
            yaml_contents: invalid yaml as a string
            exception: an exception class
        """
        with patch('__builtin__.open', create=True) as mock_open:
            mock_open.return_value = MagicMock(spec=file)
            manager = mock_open.return_value.__enter__.return_value
            manager.read.return_value = yaml_contents
            from furious.config import _parse_yaml_config

            self.assertRaises(exception, _parse_yaml_config)

    def test_get_config_invalid_yaml(self):
        from furious.config import InvalidYamlFile

        os.path.exists = Mock(return_value=True)
        example_yaml = str('secret_key:"blah"\n'
                           'persistence:bubble\n'
                           'task_system:flah\n')
        self.mock_get_config_raises(example_yaml, InvalidYamlFile)

    def test_get_persistence_module(self):
        from furious.config import get_persistence_module
        from furious import config
        module = get_persistence_module('furious.config')
        self.assertEqual(module, config)

    def test_get_config_empty_yaml(self):
        from furious.config import EmptyYamlFile

        os.path.exists = Mock(return_value=True)
        example_yaml = str('')
        self.mock_get_config_raises(example_yaml, EmptyYamlFile)

    def test_get_config_with_no_yaml_file(self):
        os.path.exists = Mock(return_value=False)
        from furious.config import _parse_yaml_config

        config = _parse_yaml_config()

        self.assertEqual(config, {'secret_key':
                                  '931b8-i-f44330b4a5-am-3b9b733f-not-secure-043e96882',
                                  'persistence': 'ndb',
                                  'task_system': 'appengine_taskqueue'})
