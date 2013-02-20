#
# Copyright 2013 WebFilings, LLC
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
import logging

from google.appengine.ext import testbed
from mock import patch
import unittest


class TestPersistMarker(unittest.TestCase):
    def setUp(self):
        import os
        import uuid

        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_taskqueue_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_datastore_v3_stub()

        # Ensure each test looks like it is in a new request.
        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    def test_store_context(self):
        from furious.extras.appengine.ndb_persistence import store_context
        from furious.extras.appengine.ndb_persistence import load_context
        from furious.extras.appengine.ndb_persistence import ContextPersist
        from furious.context import Context
        ctx = Context()
        ctx.add('test', args=[1, 2])
        ctx_dict = ctx.to_dict()
        store_context(ctx.id, ctx_dict)
        ctx_persisted = ContextPersist.get_by_id(ctx.id)
        self.assertIsNotNone(ctx_persisted)
        reloaded_ctx = load_context(ctx.id)
        self.assertEqual(reloaded_ctx, ctx_dict)


    def tearDown(self):
        self.testbed.deactivate()
