
try:
    import json
except ImportError:
    import simplejson as json

import unittest

from mock import patch


class TestDefaultsDecorator(unittest.TestCase):
    """Ensure that defaults decorator works as expected."""

    def test_decorated_name_is_preserved(self):
        """Ensure defaults decorator sets options on decorated function."""
        from furious.async import defaults

        @defaults(test=None)
        def decorated_function():
            pass

        self.assertEqual('decorated_function', decorated_function.__name__)

    def test_decorate_with_options(self):
        """Ensure defaults decorator sets options on decorated function."""
        from furious.async import defaults

        options = {'test': 'me'}

        @defaults(**options)
        def decorated_function():
            pass

        self.assertEqual(options, decorated_function._async_options)

    def test_raises_on_bad_option(self):
        """Ensure defaults decorator sets options on decorated function."""
        from furious.async import defaults

        options = {'job': 'me'}

        self.assertRaises(AssertionError, defaults, **options)

    def test_raises_on_good_with_bad_options(self):
        """Ensure defaults decorator sets options on decorated function."""
        from furious.async import defaults

        options = {'job': 'me', 'other': 'option'}

        self.assertRaises(AssertionError, defaults, **options)


class TestAsync(unittest.TestCase):
    """Make sure Async produces correct Task objects."""

    def test_update_options(self):
        """Ensure update_options updates the options."""
        from furious.async import Async

        options = {'value': 1, 'other': 'zzz', 'nested': {1: 1}}

        job = Async()
        job.update_options(**options)

        self.assertEqual(options, job._options)

    def test_get_options(self):
        """Ensure get_options returns the job options."""
        from furious.async import Async

        options = {'value': 1, 'other': 'zzz', 'nested': {1: 1}}

        job = Async()
        job._options = options

        self.assertEqual(options, job.get_options())

    def test_set_job(self):
        """Ensure set_job correctly updates options and function path."""
        from furious.async import Async

        function = "test.func"

        job = Async()
        job.set_job(function)

        self.assertEqual(function, job._function_path)
        self.assertEqual((function, (), {}), job._options['job'])

    def test_init_with_job(self):
        """Ensure set_job correctly updates options and function path."""
        from furious.async import Async

        function = "test.func"

        job = Async(job=function)

        self.assertEqual(function, job._function_path)
        self.assertEqual((function, None, None), job._options['job'])

    def test_get_headers(self):
        """Ensure get_headers returns the job headers."""
        from furious.async import Async

        headers = {'other': 'zzz', 'nested': 1}
        options = {'headers': headers}

        job = Async(**options)

        self.assertEqual(headers, job.get_headers())

    def test_get_empty_headers(self):
        """Ensure get_headers returns the job headers."""
        from furious.async import Async

        job = Async()

        self.assertEqual({}, job.get_headers())

    def test_get_queue(self):
        """Ensure get_queue returns the job queue."""
        from furious.async import Async

        queue = "test"

        job = Async(queue=queue)

        self.assertEqual(queue, job.get_queue())

    def test_get_default_queue(self):
        """Ensure get_queue returns the default queue if non was given."""
        from furious.async import Async

        job = Async()

        self.assertEqual('default', job.get_queue())

    def test_get_task_args(self):
        """Ensure get_task_args returns the job task_args."""
        from furious.async import Async

        task_args = {'other': 'zzz', 'nested': 1}
        options = {'task_args': task_args}

        job = Async(**options)

        self.assertEqual(task_args, job.get_task_args())

    def test_get_empty_task_args(self):
        """Ensure get_task_args returns {} if no task_args."""
        from furious.async import Async

        job = Async()

        self.assertEqual({}, job.get_task_args())

    def test_to_dict(self):
        """Ensure to_dict returns a dictionary representation of the Async."""
        from furious.async import Async

        task_args = {'other': 'zzz', 'nested': 1}
        headers = {'some': 'thing', 'fun': 1}
        job = ('test', None, None)
        options = {'job': job, 'headers': headers, 'task_args': task_args}

        job = Async(**options)

        self.assertEqual(options, job.to_dict())

    def test_from_dict(self):
        """Ensure from_dict returns the correct Async object."""
        from furious.async import Async

        headers = {'some': 'thing', 'fun': 1}
        job = ('test', None, None)
        task_args = {'other': 'zzz', 'nested': 1}

        options = {'job': job, 'headers': headers, 'task_args': task_args}

        async_job = Async.from_dict(options)

        self.assertEqual(headers, async_job.get_headers())
        self.assertEqual(task_args, async_job.get_task_args())
        self.assertEqual(job[0], async_job._function_path)

    def test_reconstitution(self):
        """Ensure to_dict(job.from_dict()) returns the same thing."""
        from furious.async import Async

        headers = {'some': 'thing', 'fun': 1}
        job = ('test', None, None)
        task_args = {'other': 'zzz', 'nested': 1}
        options = {'job': job, 'headers': headers, 'task_args': task_args}

        async_job = Async(**options)

        self.assertEqual(options, async_job.to_dict())

    def test_to_task(self):
        """Ensure to_task produces the right task object."""
        import datetime
        import time

        from furious.async import Async
        from furious.async import ASYNC_ENDPOINT

        # This just drops the microseconds.  It is a total mess, but is needed
        # to handle all the rounding crap.
        eta = datetime.datetime.now() + datetime.timedelta(30)
        eta_posix = time.mktime(eta.timetuple())
        eta = datetime.datetime.fromtimestamp(eta_posix)

        headers = {'some': 'thing', 'fun': 1}

        job = ('test', None, None)

        expected_url = "%s/%s" % (ASYNC_ENDPOINT, 'test')

        task_args = {'eta': eta}
        options = {'job': job, 'headers': headers, 'task_args': task_args}

        task = Async(**options).to_task()

        # App Engine sets this header by default.
        full_headers = {
            'X-AppEngine-Current-Namespace': ''
        }
        full_headers.update(headers)

        self.assertEqual(eta_posix, task.eta_posix)
        self.assertEqual(expected_url, task.url)
        self.assertEqual(full_headers, task.headers)

        self.assertEqual(
            options, Async.from_dict(json.loads(task.payload)).get_options())


class TestRunJob(unittest.TestCase):
    """Test that run_job correctly executes functions from Async options."""

    @patch('__builtin__.dir')
    def test_runs_with_none_arg(self, dir_mock):
        """Ensure run_job calls with None arg."""
        from furious.async import Async
        from furious.async import run_job

        work = Async(job=("dir", [None]))

        run_job(work)

        dir_mock.assert_called_once_with(None)

    @patch('__builtin__.dir')
    def test_runs_with_none_kwarg(self, dir_mock):
        """Ensure run_job calls with a kwarg=None."""
        from furious.async import Async
        from furious.async import run_job

        work = Async(job=("dir", {'something': None}))

        run_job(work)

        dir_mock.assert_called_once_with(something=None)

    @patch('__builtin__.dir')
    def test_runs_with_non_arg_and_kwarg(self, dir_mock):
        """Ensure run_job calls with a None arg and kwarg=None."""
        from furious.async import Async
        from furious.async import run_job

        work = Async(job=("dir", [None], {'something': None}))

        run_job(work)

        dir_mock.assert_called_once_with(None, something=None)

    def test_raises_on_missing_job(self):
        """Ensure run_job raises an exception on bogus standard import."""
        from furious.async import Async
        from furious.async import run_job

        work = Async()

        self.assertRaisesRegexp(
            Exception, "contains no job to execute",
            run_job, work)


class TestGetFunctionReference(unittest.TestCase):
    """Test that _get_function_reference can find and load functions."""

    @patch('__builtin__.dir')
    def test_runs_builtin(self, dir_mock):
        """Ensure run_job is able to correctly run a builtin."""
        from furious.async import _get_function_reference

        function = _get_function_reference("dir")

        self.assertIs(dir_mock, function)

    def test_raises_on_bogus_builtin(self):
        """Ensure run_job raises an exception on bogus builtin."""
        from furious.async import _get_function_reference

        self.assertRaisesRegexp(
            Exception, "Unable to find function",
            _get_function_reference, "something_made_up")

    @patch('email.parser.Parser')
    def test_runs_std_imported(self, parser_mock):
        """Ensure run_job is able to correctly run bundled python functions."""
        from furious.async import _get_function_reference

        function = _get_function_reference("email.parser.Parser")

        self.assertIs(parser_mock, function)

    def test_raises_on_bogus_std_imported(self):
        """Ensure run_job raises an exception on bogus standard import."""
        from furious.async import _get_function_reference

        self.assertRaisesRegexp(
            Exception, "Unable to find function",
            _get_function_reference, "email.parser.NonExistentThing")

