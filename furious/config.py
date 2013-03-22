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
from StringIO import StringIO

import yaml


FURIOUS_YAML_NAMES = ['furious.yaml', 'furious.yml']

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


class MissingYamlFile(Exception):
    """
    furious.yaml cannot be found
    """


def module_import(module_path):
    """Imports the module indicated in name

    :param module_path: :class: `str` representing a module path such as
        'furious.config' or 'furious.extras.appengine.ndb_persistence'

    :return: :class: `module` module matching name of the last component,
        ie: for 'furious.extras.appengine.ndb_persistence' it returns a
        reference to ndb_persistence

    :raises: :class: `BadModulePathError` if the module is not found
    """
    try:
        # Import whole module path.
        module = __import__(module_path)

        # Split into components: ['furious',
        # 'extras','appengine','ndb_persistence'].
        components = module_path.split('.')

        # Starting at the second component, set module to a
        # a reference to that component. at the end
        # module with be the last component. In this case:
        # ndb_persistence
        for component in components[1:]:
            module = getattr(module, component)
        return module
    except ImportError:
        raise BadModulePathError(
            'Unable to find module "%s".' % (module_path,))


def get_persistence_module(name, known_modules=PERSISTENCE_MODULES):
    """Get a known persistence module or one where name is a module path

    :param name: :class: `str` name of persistence module.

    :param known_modules: :class: `dict` module names and module paths,
        ie: {'ndb':'furious.extras.appengine.ndb_persistence'}

    :return: :class: `module` module of the module path matching
        the name in known_modules or the module path that is name
    """
    module_path = known_modules.get(name) or name
    module = module_import(module_path=module_path)
    return module


def get_default_persistence_engine(known_modules=PERSISTENCE_MODULES):
    config = get_config()
    return get_persistence_module(config['persistence'],
                                  known_modules=known_modules)


def find_furious_yaml(config_file=__file__):
    """
    Traverse directory trees to find a furious.yaml file

    Begins with the location of this file then checks the
    working directory if not found

    :param config_file: :class: `str` location of this file,
        override for testing

    :return: :class: `str` path of furious.yaml or None
        if not found
    """
    checked = set()
    yaml = _find_furious_yaml(os.path.dirname(config_file), checked)
    if not yaml:
        yaml = _find_furious_yaml(os.getcwd(), checked)
    return yaml


def _find_furious_yaml(start, checked):
    """Traverse the directory tree identified by start
    until a directory already in checked is encountered or the path
    of furious.yaml is found.

    Checked is present both to make the loop termination easy
    to reason about and so the same directories do not get
    rechecked

    :param start: :class: `str` the path to start looking in
        and work upward from checked: the set of already
        checked directories

    :return: :class: `str` the path of the furious.yaml
        file or None if it is not found
    """
    directory = start
    while directory not in checked:
        checked.add(directory)
        for fs_yaml_name in FURIOUS_YAML_NAMES:
            yaml_path = os.path.join(directory, fs_yaml_name)
            if os.path.exists(yaml_path):
                return yaml_path
        directory = os.path.dirname(directory)
    return None


def default_config():
    """The default configuration allows furious to work
    even without a user furious.yaml

    :return: :class: `dict` of defaults used by various parts of furious
    """
    return {'secret_key':
            '931b8-i-f44330b4a5-am-3b9b733f-not-secure-043e96882',
            'persistence': 'ndb',
            'task_system': 'appengine_taskqueue'}


def _load_yaml_config(path=None):
    """Open and return the yaml contents."""
    furious_yaml_path = path or find_furious_yaml()
    if furious_yaml_path is None:
        logging.debug("furious.yaml not found.")
        return None

    with open(furious_yaml_path) as yaml_file:
        return yaml_file.read()


def _parse_yaml_config(config_data=None):
    """
    Gets the configuration from the found furious.yaml
    file and parses the data.

    :return: :class: `dict` parsed from the yaml file
    """
    data_map = default_config()

    # If we were given config data to use, use it.  Otherwise, see if there is
    # a furious.yaml to read the config from.
    config_data = config_data or _load_yaml_config()
    if config_data is None:
        logging.debug("No custom furious config, using default config.")
        return data_map

    # TODO: validate the yaml contents
    config = yaml.safe_load(config_data)

    # If there was a valid custom config, it will be a dict.  Otherwise,
    # ignore it.
    if isinstance(config, dict):
        # Apply the custom config over the default config.  This allows us to
        # extend functionality without breaking old stuff.
        data_map.update(config)
    elif not None:
        raise InvalidYamlFile("The furious.yaml file "
                              "is invalid yaml")

    return data_map


def get_config():
    return _config

_config = _parse_yaml_config()

