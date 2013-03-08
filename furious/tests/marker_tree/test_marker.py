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
from mock import patch
from furious.marker_tree.marker import Marker


class TestMarker(unittest.TestCase):
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

    @patch("furious.marker_tree.identity_utils.InvalidGroupId", autospec=True)
    def test_async_to_marker(self, invalid_group_id):
        from furious.async import Async

        task = Async(target=dir)
        task.id = "1"

        marker = Marker.from_async(task)

        self.assertIsNone(marker.group_id)

    def test_is_marker_leaf(self):
        """
        Make sure a marker can report if it is a leaf marker
        or not
        """
        from furious.marker_tree.identity_utils import leaf_persistence_id_from_group_id

        root_marker = Marker(id="polly")
        for x in xrange(3):
            root_marker.children.append(
                Marker(id=leaf_persistence_id_from_group_id(
                       root_marker.id, x)))

        originally_a_leaf_marker = root_marker.children[0]

        sub_tree_marker = Marker(
            id=originally_a_leaf_marker.id,
            children=[Marker(id=leaf_persistence_id_from_group_id(
                             originally_a_leaf_marker.id, i))
                      for i in xrange(3)])

        root_marker.children[0] = sub_tree_marker

        still_leaf_marker = root_marker.children[1]
        now_sub_tree_marker = root_marker.children[0]
        self.assertTrue(still_leaf_marker.is_leaf())
        self.assertFalse(now_sub_tree_marker.is_leaf())

    def test_get_multi(self):
        from furious.marker_tree.identity_utils import leaf_persistence_id_from_group_id

        root_marker = Marker(id="freddy")
        for x in xrange(3):
            root_marker.children.append(
                Marker(id=leaf_persistence_id_from_group_id(
                       root_marker.id, x)))

        root_marker._persist_whole_graph()

        markers = Marker.get_multi([child.id for child in
                                    root_marker.children])

        self.assertEqual(len(markers), 3)

        markers = Marker.get_multi([root_marker.children[0].id,
                                    root_marker.children[1].id, "foobar"])

        self.assertEqual(len(markers), 2)

    def test_get_marker_tree_leaves(self):
        """
        Make sure all the leaves of a marker are returned
        as expected from marker._list_of_leaf_markers()
        """
        from furious.marker_tree.identity_utils import leaf_persistence_id_from_group_id

        root_marker = Marker(id="polly")
        for x in xrange(3):
            root_marker.children.append(
                Marker(id=leaf_persistence_id_from_group_id(
                       root_marker.id, x)))

        originally_a_leaf_marker = root_marker.children[0]

        sub_tree_marker = Marker(id=originally_a_leaf_marker.id,
                                 children=[Marker(id=
                                 leaf_persistence_id_from_group_id(originally_a_leaf_marker.id, i))
                                           for i in xrange(3)])

        root_marker.children[0] = sub_tree_marker
        root_marker.persist()

        leaves = root_marker._list_of_leaf_markers()
        self.assertEqual(len(leaves), 5)

        reloaded_root_marker = Marker.get(root_marker.id)
        self.assertIsNotNone(reloaded_root_marker)
        leaves = reloaded_root_marker._list_of_leaf_markers()
        #no jobs run and updated, so there should be no
        #leaves persisted yet
        self.assertEqual(len(leaves), 0)

    def test_individual_serialization(self):
        """
        Make sure a marker with children as IDs
        which is the state they would be in after loading
        from the persistence layer, the children maintain
        through serialization
        """
        from furious.marker_tree.identity_utils import leaf_persistence_id_from_group_id
        from furious.job_utils import encode_callbacks
        from furious.tests.marker_tree import dummy_success_callback

        marker = Marker.from_dict(
            {'id': 'test', 'callbacks':
             encode_callbacks({'success': dummy_success_callback})})
        self.assertEqual(marker.id, 'test')
        marker2 = Marker.from_dict(marker.to_dict())
        self.assertEqual(marker2.to_dict(), marker.to_dict())

        root_marker = Marker(id="fun")
        children = []
        for x in xrange(10):
            children.append(
                Marker(id=
                       leaf_persistence_id_from_group_id(
                       root_marker.id, x)))

        root_marker.children = [marker.id for marker in children]

        root_dict = root_marker.to_dict()

        self.assertTrue('children' in root_dict.keys())
        self.assertEqual(len(children), len(root_dict['children']))

        for index, child_id in enumerate(root_dict['children']):
            self.assertEqual(children[index].id, child_id)

        reconstituted_root = Marker.from_dict(root_dict)

        self.assertEqual(len(reconstituted_root.children),
                         len(reconstituted_root.children_to_dict()))

    def test_graph_serialization(self):
        """
        Make sure when a marker tree graph is serialized
        (to_dict), it gets deserialized(from_dict) with
        all it's children intact as Markers
        """
        from furious.marker_tree.identity_utils import leaf_persistence_id_from_group_id

        root_marker = Marker(id="jolly")
        for x in xrange(3):
            root_marker.children.append(
                Marker(id=leaf_persistence_id_from_group_id(
                    root_marker.id, x)))

        root_dict = root_marker.to_dict()

        self.assertTrue('children' in root_dict.keys())
        self.assertEqual(len(root_marker.children), len(root_dict['children']))

        reconstituted_root = Marker.from_dict(root_dict)

        self.assertIsInstance(reconstituted_root, Marker)
        self.assertEqual(len(reconstituted_root.children),
                         len(root_marker.children))
        self.assertEqual(len(reconstituted_root.children),
                         len(reconstituted_root.children_to_dict()))
        for child in reconstituted_root.children:
            self.assertIsInstance(child, Marker)

    def test_get_group_id_from_group_id(self):
        """
        Make sure all children can id their parent marker
        when they have an independent id, but passed the parent
        ID as a group_id
        """

        root_marker = Marker(id="polly")
        for x in xrange(2):
            root_marker.children.append(Marker(id=str(x),
                                               group_id=root_marker.id))

        for child in root_marker.children:
            self.assertEqual(child.get_group_id(), "polly")

    def test_get_group_id_from_leaf(self):
        """
        Make sure all children can id their parent marker
        when their id was created by prefixing with parent id
        """
        from furious.marker_tree.identity_utils import leaf_persistence_id_from_group_id

        root_marker = Marker(id="polly")
        for x in xrange(3):
            root_marker.children.append(
                Marker(id=leaf_persistence_id_from_group_id(
                    root_marker.id, x)))

        for child in root_marker.children:
            self.assertEqual(child.get_group_id(), "polly")

    def test_persist_marker_tree_graph(self):
        """
        Make sure when a marker tree is persisted,
        it only saves the non-leaf nodes and the
        children properties contains only IDs
        """
        from furious.marker_tree.identity_utils import leaf_persistence_id_from_group_id

        root_marker = Marker(id="peanut")
        for x in xrange(3):
            root_marker.children.append(
                Marker(id=leaf_persistence_id_from_group_id(
                    root_marker.id, x)))

        root_marker.persist()

        loaded_marker = Marker.get(root_marker.id)

        self.assertIsNotNone(loaded_marker)
        self.assertEqual(root_marker.id, loaded_marker.id)
        self.assertEqual(root_marker.done, loaded_marker.done)
        self.assertEqual(len(root_marker.children),
                         len(loaded_marker.children))

        children_ids = [child.id for child in root_marker.children]
        loaded_children_ids = loaded_marker.children

        for index, idx in enumerate(children_ids):
            self.assertEqual(idx, loaded_children_ids[index])

        loaded_child_marker = Marker.get(root_marker.children[0].id)

        self.assertIsNone(loaded_child_marker)

    @patch('furious.context.get_current_context', auto_spec=True)
    def test_persist_tree_after_tasks_inserted(self,
                                               mock_get_current_context):
        """
        Make sure a marker tree isn't(Raises exception)
        persisted after the current context's tasks have
        been inserted
        """
        from furious.context.context import Context
        from furious.marker_tree.identity_utils import leaf_persistence_id_from_group_id
        from furious.marker_tree.exceptions import NotSafeToSave

        a_context = Context(id="zebra")
        a_context._tasks_inserted = True
        mock_get_current_context.return_value = a_context
        root_marker = Marker(id="zebra")
        for x in xrange(3):
            root_marker.children.append(
                Marker(id=leaf_persistence_id_from_group_id(
                    root_marker.id, x)))

        self.assertRaises(NotSafeToSave, root_marker.persist)

    def test_persist_internal_node_marker(self):
        """
        Make sure internal nodes are saved during the persistence
        of a marker tree graph.
        """
        from furious.marker_tree.identity_utils import leaf_persistence_id_from_group_id

        root_marker = Marker(id="cracker")
        for x in xrange(2):
            root_marker.children.append(Marker(
                id=str(x),
                group_id=root_marker.id,
                children=[
                    Marker(id=leaf_persistence_id_from_group_id(str(x), i))
                    for i in xrange(3)]
            ))

        root_marker.persist()
        internal_node1 = root_marker.children[0]
        leaf_node2 = internal_node1.children[1]
        loaded_internal = Marker.get(internal_node1.id)

        self.assertIsNotNone(loaded_internal)

        loaded_leaf = Marker.get(leaf_node2.id)

        self.assertIsNone(loaded_leaf)

    def test_persist_leaf_marker(self):
        """
        Make sure a leaf marker is saved when it persists
        itself but only during the update_done process.
        Make sure that loaded leaf marker can id it's parent
        marker
        """
        from furious.marker_tree.identity_utils import leaf_persistence_id_from_group_id
        from furious.marker_tree.exceptions import NotSafeToSave

        root_marker = Marker(id="heart")
        for x in xrange(3):
            root_marker.children.append(
                Marker(id=leaf_persistence_id_from_group_id(
                    root_marker.id, x)))

        leaf_marker = root_marker.children[0]
        self.assertRaises(NotSafeToSave, leaf_marker.persist)
        leaf_marker._update_done_in_progress = True
        leaf_marker.persist()
        loaded_marker = Marker.get(leaf_marker.id)

        self.assertIsNotNone(loaded_marker)
        self.assertIsInstance(loaded_marker, Marker)
        self.assertTrue(loaded_marker.get_group_id(), 'heart')

    @patch('furious.marker_tree.marker.count_marked_as_done',
           auto_spec=True)
    @patch('furious.marker_tree.marker.count_update', auto_spec=True)
    def test_update_done_of_leaf_travels_to_root_when_last(
            self,
            mock_count_update,
            mock_count_marked_as_done):
        """
        Make sure when all but one marker is done and it
        runs an update_done, the processes will bubble
        up to the root marker and it's update done will be called.
        How many times is Marker.update_done called?

           2
           \
         ------
         \     \
         3      3
        ----   ----
        \ \ \  \ \ \
        1 1 1  1 1 1

        Each leaf node calls itself once.

        Each internal vertex is called during bubble_up_done
        of each child.

        The first time the root marker.update_done is run, the
        right child node is not done
        """
        mock_count_update.return_value = None
        from furious.marker_tree.identity_utils import leaf_persistence_id_from_group_id
        from furious.tests.marker_tree import dummy_success_callback
        from furious.tests.marker_tree import dummy_internal_vertex_combiner
        from furious.tests.marker_tree import dummy_leaf_combiner

        context_callbacks = {
            'success': dummy_success_callback,
            'internal_vertex_combiner': dummy_internal_vertex_combiner,
            'leaf_combiner': dummy_leaf_combiner
        }
        root_marker = Marker(id="delve",
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

        root_marker.persist()

        with patch('furious.tests.marker_tree.'
                   'dummy_success_callback', auto_spec=True) \
                as mock_success_callback:
            with patch('furious.tests.marker_tree.'
                       'dummy_leaf_combiner', auto_spec=True) \
                    as mock_leaf_combiner:
                with patch('furious.tests.marker_tree.'
                           'dummy_internal_vertex_combiner', auto_spec=True) \
                        as mock_internal_vertex_combiner:

                    mock_leaf_combiner.return_value = ["1"]
                    mock_internal_vertex_combiner.return_value = ["2"]

                    for internal_node in root_marker.children:
                        for leaf_node in internal_node.children:
                            leaf_node.done = True
                            leaf_node.result = 1
                            leaf_node.update_done(persist_first=True)

                    loaded_root_marker = Marker.get("delve")
                    self.assertTrue(loaded_root_marker.done)
                    self.assertEqual(mock_count_update.call_count, 14)
                    #9 is the number of nodes in the graph
                    self.assertEqual(mock_count_marked_as_done.call_count, 9)

                    #pretend a task was run again later on after
                    #the root had succeeded, it should only
                    #reach it's parent node and that should
                    #not bubble up
                    leaf_node = root_marker.children[0].children[1]
                    leaf_node.update_done(persist_first=True)

                    self.assertEqual(mock_count_update.call_count, 16)
                    mock_success_callback.assert_called_once_with("delve", ['2'])

                    #one for each non-leaf node
                    self.assertEqual(mock_internal_vertex_combiner.call_count, 3)
                    self.assertEqual(mock_leaf_combiner.call_count, 2)

    def test_load_whole_graph(self):
        root_marker = Marker(id="delve")
        for x in xrange(2):
            root_marker.children.append(Marker(
                id=str(x),
                group_id=root_marker.id
            ))

        root_marker._persist_whole_graph()

        re_root_marker = Marker.get(root_marker.id)

        re_root_marker._load_whole_graph()

        self.assertEqual(len(re_root_marker.children), 2)
        self.assertTrue(hasattr(re_root_marker.children[0], 'id'))
        self.assertTrue(hasattr(re_root_marker.children[1], 'id'))
