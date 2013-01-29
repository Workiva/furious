import logging
import os
import yaml

FURIOUS_YAML_NAMES = ['furious.yaml','furious.yml']

PERSISTENCE_MODULES = {
    'ndb': 'furious.extras.appengine.ndb'
}

class BadModulePathError(Exception):
    """Invalid module path."""

class InvalidPersistenceModuleName(Exception):
    """
    There is no persistence strategy by that name
    """

def module_import(name):
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod,comp)
    return mod

def get_persistence_module(name):
    module_path = PERSISTENCE_MODULES.get(name)
    module = None
    if not module_path:
        raise InvalidPersistenceModuleName(
            'no persistence strategy by the name %s'%(name,))
    try:
        module = module_import(name=module_path)
    except ImportError:
        raise BadModulePathError(
            'Unable to find module "%s".' % (module_path,))
    return module

def get_configured_persistence_module():
    return get_persistence_module(config['persistence'])

class MissingYamlFile(Exception):
    """
    furious.yaml cannot be found
    """

def find_furious_yaml(status_file=__file__):
    """
    Traverse directory trees to find a furious.yaml file

    Begins with the location of this file then checks the
    working directory if not found

    Args:
        status_file: location of this file, override for
        testing
    Returns:
        the path of furious.yaml or None if not found
    """
    checked = set()
    yaml = _find_furious_yaml(os.path.dirname(status_file),checked)
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
    return {'secret_key': '931b8-i-f44330b4a5-am-3b9b733f-not-secure-043e96882',
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
    if not furious_yaml_path:
#        raise MissingYamlFile("furious.yaml is missing")
        logging.warning("furious.yaml is missing, using default config")
    else:
        f = open(furious_yaml_path)
#        TODO: validate the yaml contents
        data = yaml.load(f)
        dataMap.update(data)
        f.close()
    return dataMap

config = get_config()
