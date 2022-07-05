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

FURIOUS_YAML_NAMES = ['furious.yaml', 'furious.yml']

PERSISTENCE_MODULES = {
    'ndb': 'furious.extras.appengine.ndb_persistence'
}


class BadModulePathError(Exception):
    """Invalid module path."""


class InvalidPersistenceModuleName(Exception):
    """There is no persistence strategy by that name."""


class InvalidYamlFile(Exception):
    """The furious.yaml file is invalid yaml."""


class EmptyYamlFile(Exception):
    """The furious.yaml file is empty."""


class MissingYamlFile(Exception):
    """furious.yaml cannot be found."""


def get_default_persistence_engine(known_modules=PERSISTENCE_MODULES):
    """Return the default persistence engine set in furious.yaml."""
    return _get_configured_module('persistence', known_modules=known_modules)


def get_completion_cleanup_queue():
    """Get the default queue that completion should use to cleanup markers on.
    """
    config = get_config()
    return config.get('cleanupqueue')


def get_completion_default_queue():
    """Get the default queue that completion should use to cleanup markers on.
    """
    config = get_config()
    return config.get('defaultqueue')


def get_completion_cleanup_delay():
    """Get the default queue that completion should use to cleanup markers on.
    """
    config = get_config()
    return config.get('cleanupdelay')


def get_csrf_check():
    """Get the CSRF check function, which takes one arg (webapp2.Reqeust)
    and returns None, throwing an exception if a CSRF attack is detected.
    """
    from furious.job_utils import path_to_reference

    return path_to_reference(get_config().get('csrf_check'))


def _get_configured_module(option_name, known_modules=None):
    """Get the module specified by the value of option_name. The value of the
    configuration option will be used to load the module by name from the known
    module list or treated as a path if not found in known_modules.
    Args:
        option_name: name of persistence module
        known_modules: dictionary of module names and module paths,
            ie: {'ndb':'furious.extras.appengine.ndb_persistence'}
    Returns:
        module of the module path matching the name in known_modules
    """
    from furious.job_utils import path_to_reference

    config = get_config()
    option_value = config[option_name]

    # If no known_modules were give, make it an empty dict.
    if not known_modules:
        known_modules = {}

    module_path = known_modules.get(option_value) or option_value
    return path_to_reference(module_path)


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
    result = _find_furious_yaml(os.path.dirname(config_file), checked)
    if not result:
        result = _find_furious_yaml(os.getcwd(), checked)
    return result


def _find_furious_yaml(start, checked):
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

    Returns:
        dictionary of defaults used by various parts of furious
    """
    return {'secret_key':
            '931b8-i-f44330b4a5-am-3b9b733f-not-secure-043e96882',
            'persistence': 'ndb',
            'cleanupqueue': 'default',
            'cleanupdelay': 7600,
            'defaultqueue': 'default',
            'task_system': 'appengine_taskqueue',
            'csrf_check': 'furious.csrf_check'}


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
    Returns:
        a dictionary parsed from the yaml file
    """
    data_map = default_config()

    # If we were given config data to use, use it.  Otherwise, see if there is
    # a furious.yaml to read the config from.  Note that the empty string will
    # result in the default config being used.
    if config_data is None:
        config_data = _load_yaml_config()

    if not config_data:
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
