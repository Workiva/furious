import unittest


def some_function():
    pass


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


