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
import unittest

from google.appengine.ext import testbed


class TestWideMarker(unittest.TestCase):
    def setUp(self):
        import os
        import uuid

        from furious.tests.marker_tree import dummy_success_callback
        from furious.tests.marker_tree import dummy_internal_vertex_combiner
        from furious.tests.marker_tree import dummy_leaf_combiner

        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_taskqueue_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_datastore_v3_stub()

        # Ensure each test looks like it is in a new request.
        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

        self.context_callbacks = {
            'success': dummy_success_callback,
            'internal_vertex_combiner': dummy_internal_vertex_combiner,
            'leaf_combiner': dummy_leaf_combiner
        }

    def tearDown(self):
        self.testbed.deactivate()

    def test_build_wide_marker_tree(self):
        from furious.async import Async
        from furious.marker_tree.wide_tree_marker import WideTreeMarker

        root_marker = WideTreeMarker(
            id="bob",
            group_id=None,
            callbacks=self.context_callbacks)

        tasks = []
        for index in xrange(95):
            tasks.append(Async(target=dir))

        root_marker.children = WideTreeMarker.make_markers_for_tasks(
            tasks,
            group_id=root_marker.id,
            context_callbacks=self.context_callbacks
        )

        self.assertEqual(root_marker.count_nodes(), 106)

    def test_build_two_level_tree(self):
        from furious.async import Async
        from furious.marker_tree.wide_tree_marker import WideTreeMarker

        root_marker = WideTreeMarker(
            id="bob",
            group_id=None,
            callbacks=self.context_callbacks)

        tasks = []
        for index in xrange(119):
            tasks.append(Async(target=dir))

        root_marker.children = WideTreeMarker.make_markers_for_tasks(
            tasks,
            group_id=root_marker.id,
            context_callbacks=self.context_callbacks
        )

        self.assertEqual(root_marker.count_nodes(), 1 + 100 + 11 + 2 + 19)
