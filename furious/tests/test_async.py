
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

    def test_none_function(self):
        """Ensure passing None as function raises."""
        from furious.async import Async
        from furious.job_utils import BadFunctionPathError

        self.assertRaises(BadFunctionPathError, Async, None)

    def test_empty_function_path(self):
        """Ensure passing None as function raises."""
        from furious.async import Async
        from furious.job_utils import BadFunctionPathError

        self.assertRaises(BadFunctionPathError, Async, '')

    def test_job_params(self):
        """Ensure good args and kwargs generate a well-formed job tuple."""
        from furious.async import Async

        job = ("test", [1, 2, 3], {'a': 1, 'b': 2, 'c': 3})
        async_job = Async(*job)

        self.assertEqual(job, async_job._options['job'])

    def test_no_args_or_kwargs(self):
        """Ensure no args and no kwargs generate a well-formed job tuple."""
        from furious.async import Async

        function = "test"
        async_job = Async(function)

        self.assertEqual((function, None, None), async_job._options['job'])

    def test_args_with_no_kwargs(self):
        """Ensure args and no kwargs generate a well-formed job tuple."""
        from furious.async import Async

        job = ("test", (1, 2, 3))
        async_job = Async(*job)

        self.assertEqual(job + (None,), async_job._options['job'])

    def test_no_args_with_kwargs(self):
        """Ensure no args with kwargs generate a well-formed job tuple."""
        from furious.async import Async

        job = ("test", None, {'a': 1, 'b': 'c', 'alpha': True})
        async_job = Async(*job)

        self.assertEqual(job, async_job._options['job'])

    def test_gets_callable_path(self):
        """Ensure the job tuple contains the callable path."""
        from furious.async import Async

        def some_function():
            """Will look like is at the module level."""
            pass

        job_args = ([1, 2, 3], {'a': 1, 'b': 2, 'c': 3})
        async_job = Async(some_function, *job_args)

        self.assertEqual(
            ('furious.tests.test_async.some_function',) + job_args,
            async_job._options['job'])

    def test_none_args_and_kwargs(self):
        """Ensure args and kwargs may be None."""
        from furious.async import Async

        job = ("something", None, None,)
        async_job = Async(*job)

        self.assertEqual(job, async_job._options['job'])

    def test_update_options(self):
        """Ensure update_options updates the options."""
        from furious.async import Async

        options = {'value': 1, 'other': 'zzz', 'nested': {1: 1}}

        job = Async("nonexistant")
        job.update_options(**options.copy())

        options['job'] = ("nonexistant", None, None)

        self.assertEqual(options, job._options)

    def test_get_options(self):
        """Ensure get_options returns the job options."""
        from furious.async import Async

        options = {'value': 1, 'other': 'zzz', 'nested': {1: 1}}

        job = Async("nonexistant")
        job._options = options

        self.assertEqual(options, job.get_options())

    def test_init_with_job(self):
        """Ensure set_job correctly updates options and function path."""
        from furious.async import Async

        function = "test.func"

        job = Async(function)

        self.assertEqual(function, job._function_path)
        self.assertEqual((function, None, None), job._options['job'])

    def test_get_headers(self):
        """Ensure get_headers returns the job headers."""
        from furious.async import Async

        headers = {'other': 'zzz', 'nested': 1}
        options = {'headers': headers}

        job = Async('nonexistant', **options)

        self.assertEqual(headers, job.get_headers())

    def test_get_empty_headers(self):
        """Ensure get_headers returns the job headers."""
        from furious.async import Async

        job = Async('nonexistant')

        self.assertEqual({}, job.get_headers())

    def test_get_queue(self):
        """Ensure get_queue returns the job queue."""
        from furious.async import Async

        queue = "test"

        job = Async('nonexistant', queue=queue)

        self.assertEqual(queue, job.get_queue())

    def test_get_default_queue(self):
        """Ensure get_queue returns the default queue if non was given."""
        from furious.async import Async

        job = Async('nonexistant')

        self.assertEqual('default', job.get_queue())

    def test_get_task_args(self):
        """Ensure get_task_args returns the job task_args."""
        from furious.async import Async

        task_args = {'other': 'zzz', 'nested': 1}
        options = {'task_args': task_args}

        job = Async('nonexistant', **options)

        self.assertEqual(task_args, job.get_task_args())

    def test_get_empty_task_args(self):
        """Ensure get_task_args returns {} if no task_args."""
        from furious.async import Async

        job = Async('nonexistant')

        self.assertEqual({}, job.get_task_args())

    def test_to_dict(self):
        """Ensure to_dict returns a dictionary representation of the Async."""
        from furious.async import Async

        task_args = {'other': 'zzz', 'nested': 1}
        headers = {'some': 'thing', 'fun': 1}
        options = {'headers': headers, 'task_args': task_args}

        job = Async('nonexistant', **options.copy())

        options['job'] = ('nonexistant', None, None)

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

        async_job = Async.from_dict(options)

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

        headers = {'some': 'thing', 'fun': 1}

        job = ('test', None, None)

        expected_url = "%s/%s" % (ASYNC_ENDPOINT, 'test')

        task_args = {'eta': eta_posix}
        options = {'job': job, 'headers': headers, 'task_args': task_args}

        task = Async.from_dict(options).to_task()

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

        work = Async("dir", [None])

        run_job(work)

        dir_mock.assert_called_once_with(None)

    @patch('__builtin__.dir')
    def test_runs_with_none_kwarg(self, dir_mock):
        """Ensure run_job calls with a kwarg=None."""
        from furious.async import Async
        from furious.async import run_job

        work = Async("dir", kwargs={'something': None})

        run_job(work)

        dir_mock.assert_called_once_with(something=None)

    @patch('__builtin__.dir')
    def test_runs_with_non_arg_and_kwarg(self, dir_mock):
        """Ensure run_job calls with a None arg and kwarg=None."""
        from furious.async import Async
        from furious.async import run_job

        work = Async("dir", [None], {'something': None})

        run_job(work)

        dir_mock.assert_called_once_with(None, something=None)

    def test_raises_on_missing_job(self):
        """Ensure run_job raises an exception on bogus standard import."""
        from furious.async import Async
        from furious.async import run_job

        work = Async("nothere")
        work._options.pop('job')
        assert 'job' not in work._options

        self.assertRaisesRegexp(
            Exception, "contains no job to execute",
            run_job, work)

