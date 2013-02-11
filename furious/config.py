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
import logging
import os
import yaml
from StringIO import StringIO

FURIOUS_YAML_NAMES = ['furious.yaml','furious.yml']

PERSISTENCE_MODULES = {
    'ndb': 'furious.extras.appengine.ndb_persistence'
}

class BadModulePathError(Exception):
    """Invalid module path."""

class InvalidPersistenceModuleName(Exception):
    """
    There is no persistence strategy by that name
    """

class InvalidYamlFile(Exception):
    """
    The furious.yaml file is invalid yaml
    """

class EmptyYamlFile(Exception):
    """
    The furious.yaml file is empty
    """

def module_import(name):
    try:
        mod = __import__(name)
        components = name.split('.')
        for comp in components[1:]:
            mod = getattr(mod,comp)
        return mod
    except ImportError:
        raise BadModulePathError(
            'Unable to find module "%s".' % (name,))

def get_persistence_module(name, valid_modules=PERSISTENCE_MODULES):
    module_path = valid_modules.get(name)
    module = None
    if not module_path:
        raise InvalidPersistenceModuleName(
            'no persistence strategy by the name %s'%(name,))

    module = module_import(name=module_path)
    return module

def get_configured_persistence_module(valid_modules=PERSISTENCE_MODULES):
    return get_persistence_module(config['persistence'],valid_modules)

class MissingYamlFile(Exception):
    """
    furious.yaml cannot be found
    """

def find_furious_yaml(config_file=__file__):
    """
    Traverse directory trees to find a furious.yaml file

    Begins with the location of this file then checks the
    working directory if not found

    Args:
        config_file: location of this file, override for
        testing
    Returns:
        the path of furious.yaml or None if not found
    """
    checked = set()
    yaml = _find_furious_yaml(os.path.dirname(config_file),checked)
    if not yaml:
        yaml = _find_furious_yaml(os.getcwd(), checked)
    return yaml

def _find_furious_yaml(start,checked):
    """Traverse the directory tree identified by start
    until a directory already in checked is encountered or the path
    of furious.yaml is found.

    Checked is present both to make the loop termination easy
    to reason about and so the same directories do not get
    rechecked

    Args:
        start: the path to start looking in and work upward from
        checked: the set of already checked directories

    Returns:
        the path of the furious.yaml file or None if it is not found
    """
    dir = start
    while dir not in checked:
        checked.add(dir)
        for fs_yaml_name in FURIOUS_YAML_NAMES:
            yaml_path = os.path.join(dir, fs_yaml_name)
            if os.path.exists(yaml_path):
                return yaml_path
        dir = os.path.dirname(dir)
    return None

def default_config():
    return {'secret_key':
            '931b8-i-f44330b4a5-am-3b9b733f-not-secure-043e96882',
            'persistence': 'ndb',
            'task_system': 'appengine_taskqueue'}


def get_config():
    """
    Gets the configuration from the found furious.yaml
    file and parses the data.
    Returns:
        a dictionary parsed from the yaml file
    """
    furious_yaml_path = find_furious_yaml()
    dataMap = default_config()
    if furious_yaml_path is None:
#        raise MissingYamlFile("furious.yaml is missing")
        logging.warning("furious.yaml is missing, using default config")
    else:
        with open(furious_yaml_path) as f:
    #        TODO: validate the yaml contents
            #load file contents into a StringIO to enable use of a
            #a mock file with yaml.load, preventing it from hanging
            sf = StringIO(f.read())
            data = yaml.load(sf)
            if not isinstance(data,dict):
                if data is None:
                    raise EmptyYamlFile("The furious.yaml file is empty")
                else:
                    raise InvalidYamlFile("The furious.yaml file "
                                          "is invalid yaml")
            dataMap.update(data)
    return dataMap

config = get_config()
