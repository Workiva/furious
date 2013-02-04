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
from mock import MagicMock
from mock import Mock
from mock import patch
import os
import re
import unittest

def splitkeepsep(s, sep):
    return reduce(lambda acc, i: acc[:-1] + [acc[-1] + i]\
    if i == sep else acc + [i],
        re.split("(%s)" % re.escape(sep), s), [])

def mock_get_config_as(yaml_contents):
    with patch('__builtin__.open', create=True) as mock_open:
        mock_open.return_value = MagicMock(spec=file)
        manager = mock_open.return_value.__enter__.return_value
        manager.__iter__.return_value = iter(splitkeepsep(
            yaml_contents, "\n"))
        manager.read.return_value = yaml_contents
        from ..config import get_config
        return get_config()

class TestConfigurationLoading(unittest.TestCase):
    def test_module_import_missing_module(self):
        from ..config import BadModulePathError
        from ..config import module_import

        self.assertRaises(BadModulePathError,
            module_import,'furious.extras.not_here')

    def test_module_import(self):
        from ..config import BadModulePathError
        from ..config import module_import
        from .. import config

        module = module_import('furious.config')
        self.assertEqual(module,config)

    def test_find_yaml(self):
        from ..config import find_furious_yaml
        config_yaml_path = find_furious_yaml()
        self.assertIsNotNone(config_yaml_path)

    def test_not_find_yaml(self):
        os.path.exists = Mock(return_value=False)
        from ..config import find_furious_yaml
        config_yaml_path = find_furious_yaml()
        self.assertIsNone(config_yaml_path)

    def test_get_config(self):
        os.path.exists = Mock(return_value=True)
        example_yaml = str('secret_key: "blah"\n'
                           'persistence: bubble\n'
                           'task_system: flah\n')

        my_config = mock_get_config_as(example_yaml)

        self.assertEqual(my_config,{'secret_key':'blah',
                                 'persistence':'bubble',
                                 'task_system':'flah'})


    def test_get_configured_persistence_exists(self):
        os.path.exists = Mock(return_value=True)
        example_yaml = str('secret_key: "blah"\n'
                           'persistence: bubble\n'
                           'task_system: flah\n')

        my_config = mock_get_config_as(example_yaml)

        from .. import config
        config.config = my_config

        persistence_module = config.get_configured_persistence_module(
            {'bubble':'furious.config'})

        self.assertEqual(persistence_module,config)


    def mock_get_config_raises(self,yaml_contents,exception=Exception):
        with patch('__builtin__.open', create=True) as mock_open:
            mock_open.return_value = MagicMock(spec=file)
            manager = mock_open.return_value.__enter__.return_value
            manager.__iter__.return_value = iter(splitkeepsep(
                yaml_contents, "\n"))
            manager.read.return_value = yaml_contents
            from ..config import get_config
            self.assertRaises(exception,get_config)

    def test_get_config_invalid_yaml(self):
        from furious.config import InvalidYamlFile
        os.path.exists = Mock(return_value=True)
        example_yaml = str('secret_key:"blah"\n'
                           'persistence:bubble\n'
                           'task_system:flah\n')
        self.mock_get_config_raises(example_yaml,InvalidYamlFile)

    def test_get_config_empty_yaml(self):
        from furious.config import EmptyYamlFile
        os.path.exists = Mock(return_value=True)
        example_yaml = str('')
        self.mock_get_config_raises(example_yaml,EmptyYamlFile)

    def test_get_config_with_no_yaml_file(self):
        os.path.exists = Mock(return_value=False)
        from ..config import get_config
        config = get_config()

        self.assertEqual(config,{'secret_key':
         '931b8-i-f44330b4a5-am-3b9b733f-not-secure-043e96882',
                                 'persistence': 'ndb',
                                 'task_system': 'appengine_taskqueue'})

