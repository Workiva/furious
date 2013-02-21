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


class TestResultsSorter(unittest.TestCase):
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

    def tearDown(self):
        self.testbed.deactivate()

    def test_ordered_grouping_results(self):
        """
        Ensure results will be combined into groups of
        combined leaf results to maintain the result
        order is the same as the tasks inserted.
        When a single task fans out, it's results
        will pulled in position

        """
        from furious.marker_tree.identity_utils import leaf_persistence_id_from_group_id
        from furious.marker_tree.result_sorter import first_iv_markers
        from furious.marker_tree.result_sorter import group_into_internal_vertex_results
        from furious.marker_tree.marker import Marker

        from furious.tests.marker_tree import dummy_leaf_combiner

        root_marker = Marker(id="little_job")
        for x in xrange(3):
            root_marker.children.append(Marker(
                id=str(x),
                group_id=root_marker.id,
                result=[2, 2, 2],
                children=[Marker(id=
                                 leaf_persistence_id_from_group_id(str(x), i),
                                 result=2)
                          for i in xrange(3)]
            ))

        for x in xrange(2):
            root_marker.children.append(Marker(
                id=leaf_persistence_id_from_group_id(root_marker.id, x + 3),
                result=1
            ))

        markers = first_iv_markers(root_marker.children)

        self.assertEqual(len(markers), 3)

        iv_results = group_into_internal_vertex_results(root_marker.children,
                                                        dummy_leaf_combiner)

        self.assertEqual(len(iv_results), 4)
        self.assertEqual(iv_results, [[2, 2, 2], [2, 2, 2], [2, 2, 2], [1, 1]])

        #shuffle children a few times
        chlin = root_marker.children
        children = [chlin[3], chlin[4], chlin[0], chlin[1], chlin[2]]
        iv_results = group_into_internal_vertex_results(children,
                                                        dummy_leaf_combiner)
        self.assertEqual(iv_results, [[1, 1], [2, 2, 2], [2, 2, 2], [2, 2, 2]])
        children = [chlin[3], chlin[0], chlin[4], chlin[1], chlin[2]]
        iv_results = group_into_internal_vertex_results(children,
                                                        dummy_leaf_combiner)
        self.assertEqual(iv_results, [[1], [2, 2, 2], [1], [2, 2, 2], [2, 2, 2]])
        iv_results = group_into_internal_vertex_results(children,
                                                        None)
        self.assertEqual(iv_results, [[2, 2, 2], [2, 2, 2], [2, 2, 2]])

    def test_combiner_results(self):
        """
        Make sure the expected results are in the root_marker.results
        after the job is done.
        """
        from furious.marker_tree.identity_utils import leaf_persistence_id_from_group_id
        from furious.marker_tree.marker import Marker

        from furious.tests.marker_tree import dummy_success_callback
        from furious.tests.marker_tree import dummy_internal_vertex_combiner
        from furious.tests.marker_tree import dummy_leaf_combiner

        context_callbacks = {
            'success': dummy_success_callback,
            'internal_vertex_combiner': dummy_internal_vertex_combiner,
            'leaf_combiner': dummy_leaf_combiner
        }
        root_marker = Marker(id="big_job",
                             callbacks=context_callbacks)
        for x in xrange(2):
            root_marker.children.append(Marker(
                id=str(x),
                group_id=root_marker.id,
                callbacks=context_callbacks,
                children=[
                    Marker(id=leaf_persistence_id_from_group_id(
                        str(x), i)) for i in xrange(3)]
            ))

        originally_a_leaf_marker = root_marker.children[1].children[1]

        sub_tree_marker = Marker(id=originally_a_leaf_marker.id,
                                 children=[Marker(id=
                                 leaf_persistence_id_from_group_id(originally_a_leaf_marker.id, i))
                                           for i in xrange(3)],
                                 callbacks=context_callbacks)

        root_marker.children[1].children[1] = sub_tree_marker
        #persist marker tree
        root_marker.persist()
        #similate running all the jobs
        leaf_markers = root_marker._list_of_leaf_markers()
        for marker in leaf_markers:
            marker.done = True
            marker.result = 1
            marker.update_done(persist_first=True)

        loaded_root_marker = Marker.get("big_job")
        self.assertTrue(loaded_root_marker.done)
        self.assertEqual(loaded_root_marker.result,
                         [[[1, 1, 1]], [[1], [[1, 1, 1]], [1]]])
