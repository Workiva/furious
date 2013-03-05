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


class TestLocalContext(unittest.TestCase):
    """Test that context._local functions correctly."""

    def setUp(self):
        """Setup the environment for a furious context."""

        import os
        import uuid

        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    def tearDown(self):
        """Cleanup our environment modifications."""

        import os

        del os.environ['REQUEST_ID_HASH']

    def test_clear_context(self):
        """Ensure clear_context successfully clears attributes set during
        initialization from the local context.
        """

        from furious.context import _local
        from furious.context._local import _clear_context
        from furious.context._local import get_local_context

        # Initialize the local context by retrieving it.
        get_local_context()

        # Make sure there is something on the local context we need to clear.
        self.assertTrue(hasattr(_local._local_context, 'registry'))

        _clear_context()

        # Make sure local context entries have been cleared
        self.assertFalse(hasattr(_local._local_context, '_executing_async'))
        self.assertFalse(
            hasattr(_local._local_context, '_executing_async_context'))
        self.assertFalse(hasattr(_local._local_context, '_initialized'))
        self.assertFalse(hasattr(_local._local_context, 'registry'))

