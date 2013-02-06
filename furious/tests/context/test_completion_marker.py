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

from google.appengine.ext import testbed
from mock import patch
from mock import Mock
from mock import MagicMock
import unittest
import uuid


class TestFunctions(unittest.TestCase):
    def test_leaf_id_functions(self):
        """
        Ensure the group id can be retrieved from the id given to an async
        """
        from furious.context.completion_marker import InvalidLeafId
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import leaf_persistence_id_to_group_id
        group_id = uuid.uuid4().hex
        index = 2
        leaf_key = leaf_persistence_id_from_group_id(group_id,index)
        self.assertIsNotNone(leaf_key)
        self.assertEqual(len(leaf_key.split(',')),2)
        reconstituted_group_id = leaf_persistence_id_to_group_id(leaf_key)
        self.assertEqual(reconstituted_group_id,group_id)

        group_id = "{0},2,4".format(uuid.uuid4().hex)
        index = 2
        leaf_key = leaf_persistence_id_from_group_id(group_id,index)
        self.assertIsNotNone(leaf_key)
        self.assertGreaterEqual(len(leaf_key.split(',')),2)
        reconstituted_group_id = leaf_persistence_id_to_group_id(leaf_key)
        self.assertEqual(reconstituted_group_id,group_id)

        def wrapper():
            return leaf_persistence_id_to_group_id("2")

        self.assertRaises(InvalidLeafId,wrapper)

        def non_string_wrapper():
            return leaf_persistence_id_to_group_id(2)

        self.assertRaises(InvalidLeafId,non_string_wrapper)

        group_id = leaf_persistence_id_to_group_id(u"234,4")
        self.assertEqual(u"234",group_id)


    def test_tree_graph_growth(self):
        """
        Test function that allows testing of the
        number of nodes in a marker tree graph
        """
        from furious.context.completion_marker import tree_graph_growth
        sizes = [tree_graph_growth(n) for n in range(0,100,10)]
        expected = [1, 11, 23, 35, 47, 59, 71, 83, 95, 107]
        self.assertEqual(sizes,expected)


    def test_initial_save_growth(self):
        """
        Test function that allows testing of the number of
        nodes which are saved when persisted
        """
        from furious.context.completion_marker import initial_save_growth
        sizes = [initial_save_growth(n) for n in range(0,100,10)]
        expected = [1, 1, 3, 5, 7, 9, 11, 13, 15, 17]
        self.assertEqual(sizes,expected)


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


    def test_do_any_have_children(self):
        """
        Make sure the static method Marker.do_any_have_children
        properly detects if any of a list of Markers have children.
        do_any_have_children is for a Marker to detect a state
        change in an idempotent manner. a child may change from
        a leaf to an node with children if it the target function
        returns a new Context.
        """
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker
        root_marker = Marker(id="fun")
        children = []
        for x in xrange(10):
            children.append(Marker(id=
            leaf_persistence_id_from_group_id(root_marker.id,x)))

        root_marker.children = [marker.id for marker in children]

        self.assertFalse(Marker.do_any_have_children(children))

        first_child = children[0]
        first_child.children=[leaf_persistence_id_from_group_id(
            first_child.id,x) for x in xrange(10)]

        self.assertTrue(Marker.do_any_have_children(children))


    def test_individual_serialization(self):
        """
        Make sure a marker with children as IDs
        which is the state they would be in after loading
        from the persistence layer, the children maintain
        through serialization
        """
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker
        marker = Marker.from_dict({'id':'test'})
        self.assertEqual(marker.id,'test')
        marker2 = Marker.from_dict(marker.to_dict())
        self.assertEqual(marker2.to_dict(),marker.to_dict())

        root_marker = Marker(id="fun")
        children = []
        for x in xrange(10):
            children.append(Marker(id=
            leaf_persistence_id_from_group_id(root_marker.id,x)))

        root_marker.children = [marker.id for marker in children]

        root_dict = root_marker.to_dict()
        self.assertTrue('children' in root_dict.keys())
        self.assertEqual(len(children),len(root_dict['children']))
        for index, child_id  in enumerate(root_dict['children']):
            self.assertEqual(children[index].id,child_id)

        reconstituted_root = Marker.from_dict(root_dict)
        self.assertEqual(len(reconstituted_root.children),
            len(reconstituted_root.children_to_dict()))


    def test_graph_serialization(self):
        """
        Make sure when a marker tree graph is serialized
        (to_dict), it gets deserialized(from_dict) with
        all it's children intact as Markers
        """
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker
        root_marker = Marker(id="jolly")
        for x in xrange(3):
            root_marker.children.append(Marker(id=
            leaf_persistence_id_from_group_id(root_marker.id,x)))

        root_dict = root_marker.to_dict()
        self.assertTrue('children' in root_dict.keys())
        self.assertEqual(len(root_marker.children),len(root_dict['children']))

        reconstituted_root = Marker.from_dict(root_dict)

        self.assertIsInstance(reconstituted_root,Marker)
        self.assertEqual(len(reconstituted_root.children),
        len(root_marker.children))
        self.assertEqual(len(reconstituted_root.children),
        len(reconstituted_root.children_to_dict()))
        for child in reconstituted_root.children:
            self.assertIsInstance(child,Marker)


    def test_get_group_id_from_group_id(self):
        """
        Make sure all children can id their parent marker
        when they have an independent id, but passed the parent
        ID as a group_id
        """
        from furious.context.completion_marker import Marker
        root_marker = Marker(id="polly")
        for x in xrange(2):
            root_marker.children.append(Marker(id=str(x),
                group_id=root_marker.id))

        for child in root_marker.children:
            self.assertEqual(child.get_group_id(),"polly")


    def test_get_group_id_from_leaf(self):
        """
        Make sure all children can id their parent marker
        when their id was created by prefixing with parent id
        """
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker
        root_marker = Marker(id="polly")
        for x in xrange(3):
            root_marker.children.append(Marker(id=
            leaf_persistence_id_from_group_id(root_marker.id,x)))

        for child in root_marker.children:
            self.assertEqual(child.get_group_id(),"polly")


    def test_persist_marker_tree_graph(self):
        """
        Make sure when a marker tree is persisted,
        it only saves the non-leaf nodes and the
        children properties contains only IDs
        """
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker
        root_marker = Marker(id="peanut")
        for x in xrange(3):
            root_marker.children.append(Marker(id=
            leaf_persistence_id_from_group_id(root_marker.id,x)))

        root_marker.persist()

        loaded_marker = Marker.get(root_marker.id)

        self.assertIsNotNone(loaded_marker)
        self.assertEqual(root_marker.id,loaded_marker.id)
        self.assertEqual(root_marker.done,loaded_marker.done)
        self.assertEqual(len(root_marker.children),
            len(loaded_marker.children))
        children_ids = [child.id for child in root_marker.children]
        loaded_children_ids = loaded_marker.children
        for index, id in enumerate(children_ids):
            self.assertEqual(id,loaded_children_ids[index])

        loaded_child_marker = Marker.get(root_marker.children[0].id)
        self.assertIsNone(loaded_child_marker)


    @patch('furious.context.get_current_context', autospec=True)
    def test_persist_tree_after_tasks_inserted(self,
                                       mock_get_current_context):
        """
        Make sure a marker tree isn't(Raises exception)
        persisted after the current context's tasks have
        been inserted
        """
        from furious.context.context import Context
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker
        from furious.context.completion_marker import NotSafeToSave
        a_context = Context(id="zebra")
        a_context._tasks_inserted = True
        mock_get_current_context.return_value = a_context
        root_marker = Marker(id="zebra")
        for x in xrange(3):
            root_marker.children.append(Marker(id=
            leaf_persistence_id_from_group_id(root_marker.id,x)))

        self.assertRaises(NotSafeToSave, root_marker.persist)


    def test_persist_internal_node_marker(self):
        """
        Make sure internal nodes are saved during the persistence
        of a marker tree graph.
        """
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker
        root_marker = Marker(id="cracker")
        for x in xrange(2):
            root_marker.children.append(Marker(
                id=str(x),
                group_id=root_marker.id,
                children=[Marker(id=
                leaf_persistence_id_from_group_id(str(x),i))
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
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker
        from furious.context.completion_marker import NotSafeToSave
        root_marker = Marker(id="heart")
        for x in xrange(3):
            root_marker.children.append(Marker(id=
            leaf_persistence_id_from_group_id(root_marker.id,x)))

        leaf_marker = root_marker.children[0]
        self.assertRaises(NotSafeToSave, leaf_marker.persist)
        leaf_marker._update_done_in_progress = True
        leaf_marker.persist()
        loaded_marker = Marker.get(leaf_marker.id)
        self.assertIsNotNone(loaded_marker)
        self.assertIsInstance(loaded_marker,Marker)
        self.assertTrue(loaded_marker.get_group_id(),'heart')


    @patch('furious.context.completion_marker.count_marked_as_done',
        autospec=True)
    @patch('furious.context.completion_marker.count_update', autospec=True)
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
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker
        context_callbacks = {'success':dummy_success_callback}
        root_marker = Marker(id="delve",
            callbacks=context_callbacks)
        for x in xrange(2):
            root_marker.children.append(Marker(
                id=str(x),
                group_id=root_marker.id,
                children=[Marker(id=
                leaf_persistence_id_from_group_id(str(x),i))
                          for i in xrange(3)]
            ))

        root_marker.persist()

        with patch('furious.tests.context.test_completion_marker.'
        'dummy_success_callback', autospec=True) as mock_success_callback:

            for internal_node in root_marker.children:
                for leaf_node in internal_node.children:
                    leaf_node.done = True
                    leaf_node.result = 1
                    leaf_node.update_done(persist_first=True)

            loaded_root_marker = Marker.get("delve")
            self.assertTrue(loaded_root_marker.done)
            self.assertEqual(mock_count_update.call_count,14)
            #9 is the number of nodes in the graph
            self.assertEqual(mock_count_marked_as_done.call_count,9)

            #pretend a task was run again later on after
            #the root had succeeded, it should only
            #reach it's parent node and that should
            #not bubble up
            leaf_node = root_marker.children[0].children[1]
            leaf_node.update_done(persist_first=True)

            self.assertEqual(mock_count_update.call_count,16)
            mock_success_callback.assert_called_once_with("delve",None)


def dummy_success_callback(id,results):
    return
