import unittest


def some_function():
    pass


class TestCheckJob(unittest.TestCase):
    """Make sure allowed forms of job tuples are supported."""

    def test_good_tuple(self):
        """Ensure good job tuple does the right thing."""
        from furious.job_utils import check_job

        job = ("test", [1, 2, 3], {'a': 1, 'b': 2, 'c': 3})
        cleansed = check_job(job)
        self.assertEqual(job, cleansed)

    def test_func_only_tuple(self):
        """Ensure check job handles function only tuple."""
        from furious.job_utils import check_job

        job = ("test",)
        cleansed = check_job(job)
        self.assertEqual(job + (None, None), cleansed)

    def test_args_only_tuple(self):
        """Ensure check job handles args only tuple."""
        from furious.job_utils import check_job

        job = ("test", [1, 2, 3])
        cleansed = check_job(job)
        self.assertEqual(job + (None,), cleansed)

    def test_kwargs_only_tuple(self):
        """Ensure job tuple with only kwargs works."""
        from furious.job_utils import check_job

        job = ("test", {'a': 1, 'b': 2, 'c': 3})
        cleansed = check_job(job)
        self.assertEqual((job[0], None, job[1]), cleansed)

    def test_gets_callable_path(self):
        """Ensure check job function returns the path of a callable."""
        from furious.job_utils import check_job

        job_args = ([1, 2, 3], {'a': 1, 'b': 2, 'c': 3})
        cleansed = check_job((some_function,) + job_args)
        self.assertEqual(
            ('furious.tests.test_job_utils.some_function',) + job_args, cleansed)

    def test_bad_func_only_tuple(self):
        """Ensure check job handles bad function only tuple."""
        from furious.job_utils import check_job
        from furious.job_utils import BadFunctionPath

        job = (None,)
        self.assertRaises(BadFunctionPath, check_job, job)

    def test_bad_job_tuple(self):
        """Ensure check job handles bad tuple."""
        from furious.job_utils import check_job
        from furious.job_utils import InvalidJobTuple

        job = ("something", None,)
        self.assertRaises(InvalidJobTuple, check_job, job)

    def test_bad_job_tuple_extra_crap(self):
        """Ensure check job handles bad tuples with too much junk."""
        from furious.job_utils import check_job
        from furious.job_utils import InvalidJobTuple

        job = ("something", None, None, None,)
        self.assertRaises(InvalidJobTuple, check_job, job)

    def test_bad_job_tuple_two_nones(self):
        """Ensure check job handles bad tuples with too much junk."""
        from furious.job_utils import check_job

        job = ("something", None, None,)
        cleansed = check_job(job)
        self.assertEqual(job, cleansed)


class TestCheckJobFunction(unittest.TestCase):
    """Make sure junk paths raise and function args get mapped to a path."""

    def test_valid_name(self):
        """Ensure check job function accepts good input."""
        from furious.job_utils import _check_job_function

        good_names = ['name', 'good.name', 'gr8.name', 'ok.na_me',
                      'Name', 'Good.Name', 'Gr8.Name', 'ok.Na_me']
        for name in good_names:
            cleansed = _check_job_function(name)
            self.assertEqual(name, cleansed)

    def test_bad_function_path(self):
        """Ensure check job function raises BadFunctionPath on bad path."""
        from furious.job_utils import _check_job_function
        from furious.job_utils import BadFunctionPath

        bad_names = ['', '0abc', 'test.0abc', 'test.ab-cd',
                     'bad%ness', '.nogood']
        for bad_name in bad_names:
            self.assertRaises(
                BadFunctionPath, _check_job_function, bad_name)

    def test_none_as_function_path(self):
        """Ensure check job function raises BadFunctionPath on missing path."""
        from furious.job_utils import _check_job_function
        from furious.job_utils import BadFunctionPath

        self.assertRaises(BadFunctionPath, _check_job_function, None)

    def test_gets_callable_path(self):
        """Ensure check job function returns the path of a callable."""
        from furious.job_utils import _check_job_function

        path = _check_job_function(some_function)
        self.assertEqual('furious.tests.test_job_utils.some_function', path)

    def test_gets_logging_path(self):
        """Ensure check job function returns the path of logging callable."""
        from furious.job_utils import _check_job_function
        import logging

        path = _check_job_function(logging.info)
        self.assertEqual('logging.info', path)

    def test_gets_builtin_path(self):
        """Ensure check job function returns the path of built-in callable."""
        from furious.job_utils import _check_job_function

        path = _check_job_function(eval)
        self.assertEqual('eval', path)


