#
# Copyright 2012 WebFilings, LLC
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

from mock import patch


class TestRunJob(unittest.TestCase):
    """Test that run_job correctly executes functions from Async options."""

    @patch('__builtin__.dir')
    def test_runs_with_none_arg(self, dir_mock):
        """Ensure run_job calls with None arg."""
        from furious.async import Async
        from furious.context import JobContext
        from furious.processors import run_job

        async = Async("dir", [None])

        with JobContext(async):
            run_job()

        dir_mock.assert_called_once_with(None)

    @patch('__builtin__.dir')
    def test_runs_with_none_kwarg(self, dir_mock):
        """Ensure run_job calls with a kwarg=None."""
        from furious.async import Async
        from furious.context import JobContext
        from furious.processors import run_job

        work = Async("dir", kwargs={'something': None})

        with JobContext(work):
            run_job()

        dir_mock.assert_called_once_with(something=None)

    @patch('__builtin__.dir')
    def test_runs_with_non_arg_and_kwarg(self, dir_mock):
        """Ensure run_job calls with a None arg and kwarg=None."""
        from furious.async import Async
        from furious.context import JobContext
        from furious.processors import run_job

        work = Async("dir", [None], {'something': None})

        with JobContext(work):
            run_job()

        dir_mock.assert_called_once_with(None, something=None)

    def test_raises_on_missing_job(self):
        """Ensure run_job raises an exception on bogus standard import."""
        from furious.async import Async
        from furious.context import NotInContextError
        from furious.processors import run_job

        work = Async("nothere")
        work._options.pop('job')
        assert 'job' not in work._options

        self.assertRaises(NotInContextError, run_job)

