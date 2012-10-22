import unittest


class TestContext(unittest.TestCase):
    """Test that the Context object functions in some basic way."""

    def test_context_works(self):
        """Ensure using a Context as a context manager works."""
        from furious.context import Context

        with Context():
            pass

    def test_add_job_to_context_works(self):
        """Ensure adding a job works."""
        from furious.async import Async
        from furious.context import Context

        with Context() as ctx:
            job = ctx.add('test', args=[1, 2])

        self.assertIsInstance(job, Async)


class TestNew(unittest.TestCase):
    """Test that new returns a new context and adds it to the registry."""

    def test_new(self):
        """Ensure new returns a new context."""
        from furious.context import Context
        from furious.context import new

        self.assertIsInstance(new(), Context)

    def test_new_adds_to_registry(self):
        """Ensure new returns a new context."""
        from furious.context import Context
        from furious.context import _local_context
        from furious.context import new

        ctx = new()

        self.assertIsInstance(ctx, Context)
        self.assertIn(ctx, _local_context.registry)

