#!/usr/bin/env python
#
# Copyright 2012 Ezox Systems LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Setup paths and App Engine's stubs, then run tests."""


import os

import sys

import argparse
import tempfile


CURRENT_PATH = os.getcwdu()

# Setup your paths here...
paths = [
    #os.path.join(CURRENT_PATH, 'lib'),
]

try:
    from dev_appserver import fix_sys_path
except ImportError:
    # Something is not setup right, maybe they're using a local .pth file.
    import site
    site.addsitedir('.')

    # Now, try again.
    from dev_appserver import fix_sys_path

fix_sys_path()

sys.path.extend(paths)

import unittest


stub_config = {
    'login_url': None,
    'require_indexes': True,
    'clear_datastore': False,
    'save_changes': False,
}


def run():
    parser = argparse.ArgumentParser(description='Run tests')
    parser.add_argument('tests', nargs='*', default='', help="Path to tests to be run.")
    parser.add_argument('--failfast', action='store_true', default=False)
    parser.add_argument('--verbosity', '-v', type=int, default=2)

    args = parser.parse_args()

    suite = _build_suite(args.tests)

    _setup_environment()

    _run_suite(suite, args)


def _run_suite(suite, options):
    runner = unittest.TextTestRunner(
        verbosity=options.verbosity,
        failfast=options.failfast)

    return runner.run(suite)


def _build_suite(tests):
    loader = unittest.defaultTestLoader
    suite = unittest.TestSuite()

    if not tests:
        suite.addTests(loader.discover(CURRENT_PATH))
    else:
        for label in tests:
            rel_root = label.replace('.', os.path.sep)

            if os.path.exists(rel_root):
                suite.addTests(loader.discover(rel_root, top_level_dir=os.getcwdu()))
            else:
                suite.addTests(loader.loadTestsFromName(label))

    return suite


def _setup_environment():
    from google.appengine.tools import dev_appserver

    config = stub_config.copy()

    config['root_path'] = os.getcwd()
    config['blobstore_path'] = tempfile.mkdtemp()
    config['datastore_path'] = tempfile.mktemp()
    config['high_replication'] = True

    dev_appserver.SetupStubs('unittest', **config)


    import logging
    logging.getLogger().setLevel(logging.DEBUG)

if __name__ == '__main__':
    run()

