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


class TestExecutionContext(unittest.TestCase):
    """Test that the ExecutionContext object functions in some basic way."""

    def setUp(self):
        import os
        import uuid

        # Ensure each test looks like it is in a new request.
        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    def test_context_requires_async(self):
        """Ensure _ExecutionContext requires an object as its first arg.
        """
        from furious.context._execution import _ExecutionContext

        self.assertRaises(TypeError, _ExecutionContext)

    def test_context_requires_async_type_arg(self):
        """Ensure _ExecutionContext requires an Async object as its first arg.
        """
        from furious.context._execution import _ExecutionContext

        self.assertRaises(TypeError, _ExecutionContext, object())

    def test_context_works(self):
        """Ensure using a _ExecutionContext as a context manager works."""
        from furious.async import Async
        from furious.context._execution import _ExecutionContext

        with _ExecutionContext(Async(target=dir)):
            pass

    def test_async_is_preserved(self):
        """Ensure _ExecutionContext exposes the async as a property."""
        from furious.async import Async
        from furious.context._execution import _ExecutionContext

        job = Async(target=dir)

        context = _ExecutionContext(job)

        self.assertIs(job, context.async)

    def test_async_is_not_settable(self):
        """Ensure _ExecutionContext async can not be set."""
        from furious.async import Async
        from furious.context._execution import _ExecutionContext

        job = Async(target=dir)

        context = _ExecutionContext(job)

        def set_job():
            context.async = None

        self.assertRaises(AttributeError, set_job)

    def test_job_added_to_local_context(self):
        """Ensure entering the context adds the job to the context stack."""
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.context._local import get_local_context

        job = Async(target=dir)
        with _ExecutionContext(job):
            self.assertIn(job, get_local_context()._executing_async)

    def test_job_removed_from_local_context(self):
        """Ensure exiting the context removes the job from the context stack.
        """
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.context._local import get_local_context

        job = Async(target=dir)
        with _ExecutionContext(job):
            pass

        self.assertNotIn(job, get_local_context()._executing_async)

    def test_job_added_to_end_of_local_context(self):
        """Ensure entering the context adds the job to the end of the job
        context stack.
        """
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.context._local import get_local_context

        job_outer = Async(target=dir)
        job_inner = Async(target=dir)
        with _ExecutionContext(job_outer):
            self.assertEqual(1, len(get_local_context()._executing_async))
            self.assertEqual(job_outer,
                             get_local_context()._executing_async[-1])

            with _ExecutionContext(job_inner):
                self.assertEqual(2, len(get_local_context()._executing_async))
                self.assertEqual(job_inner,
                                 get_local_context()._executing_async[-1])

    def test_job_removed_from_end_of_local_context(self):
        """Ensure entering the context removes the job from the end of the job
        context stack.
        """
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.context._local import get_local_context

        job_outer = Async(target=dir)
        job_inner = Async(target=dir)
        with _ExecutionContext(job_outer):
            with _ExecutionContext(job_inner):
                pass

            self.assertEqual(1, len(get_local_context()._executing_async))
            self.assertEqual(job_outer,
                             get_local_context()._executing_async[-1])

    def test_corrupt_context(self):
        """Ensure wrong context is not popped from execution context stack."""
        from furious.async import Async
        from furious.context._execution import _ExecutionContext
        from furious.context._local import get_local_context
        from furious.errors import CorruptContextError

        with self.assertRaises(CorruptContextError) as cm:
            job_outer = Async(target=dir)
            with _ExecutionContext(job_outer):
                local_context = get_local_context()
                local_context._executing_async.append(object())

        self.assertEqual((None, None, None), cm.exception.exc_info)


class TestExecutionContextFromAsync(unittest.TestCase):
    """Test that the Context object functions in some basic way."""

    def setUp(self):
        import os
        import uuid

        # Ensure each test looks like it is in a new request.
        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    def test_async_required_as_first_argument(self):
        """Ensure ExecutionContext requires an async object as its first arg.
        """
        from furious.context._execution import execution_context_from_async

        self.assertRaises(TypeError, execution_context_from_async)

    def test_async_type_required_as_first_argument(self):
        """Ensure ExecutionContext requires an Async type object as its first
        arg.
        """
        from furious.context._execution import execution_context_from_async

        self.assertRaises(TypeError, execution_context_from_async, object())

    def test_double_init_raises_error(self):
        """Ensure initing twice raises a ContextExistsError."""
        from furious.async import Async
        from furious.context._execution import execution_context_from_async
        from furious.errors import ContextExistsError

        execution_context_from_async(Async(target=dir))
        self.assertRaises(
            ContextExistsError,
            execution_context_from_async, Async(target=dir))

    def test_context_set_on_local_context(self):
        """Ensure the context is set on the local_context."""
        from furious.async import Async
        from furious.context._local import get_local_context
        from furious.context._execution import execution_context_from_async

        context = execution_context_from_async(Async(target=dir))
        self.assertIs(context, get_local_context()._executing_async_context)

