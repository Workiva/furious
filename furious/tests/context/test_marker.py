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

from furious.async import Async
from furious.context.marker import Marker, InvalidLeafId
from furious.context.marker import initial_save_growth
from furious.context.marker import make_markers_for_tasks
from furious.context.marker import tree_graph_growth
from google.appengine.ext import testbed
from mock import patch
import unittest
import uuid

class TestFunctions(unittest.TestCase):

    def test_leaf_id_functions(self):
        """
        Ensure the group id can be retrieved from the id given to an async
        """
        from furious.context.marker import leaf_persistence_id_from_group_id
        from furious.context.marker import leaf_persistence_id_to_group_id
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
        sizes = [tree_graph_growth(n) for n in range(0,100,10)]
        expected = [1, 11, 23, 35, 47, 59, 71, 83, 95, 107]
        self.assertEqual(sizes,expected)

    def test_initial_save_growth(self):
        sizes = [initial_save_growth(n) for n in range(0,100,10)]
        expected = [1, 1, 3, 5, 7, 9, 11, 13, 15, 17]
        self.assertEqual(sizes,expected)

def example_function(*args, **kwargs):
    return args



class TestMarkerTreeBuilding(unittest.TestCase):
    def setUp(self):
        import os
        import uuid

        harness = testbed.Testbed()
        harness.activate()
        harness.init_taskqueue_stub()

        # Ensure each test looks like it is in a new request.
        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    def test_marker_tree_from_async_tasks(self):
        _tasks = [
            Async(target=example_function,
                    args=[arg]) for arg in range(0,23)
        ]
        _persistence_id = uuid.uuid4().hex

        marker = Marker(id=str(_persistence_id), group_id=None,
            batch_id=_persistence_id)

        marker.children, _ = make_markers_for_tasks(_tasks, group=marker,
            batch_id=_persistence_id)

        task_count = len(_tasks)
        node_count = marker.count_nodes()
        self.assertGreater(node_count,task_count)
        self.assertEqual(node_count, tree_graph_growth(task_count))

    def test_build_tree_with_one_task(self):
        _tasks = [
        Async(target=example_function,
            args=[12])
        ]
        _persistence_id = uuid.uuid4().hex

        marker = Marker(id=str(_persistence_id), group_id=None,
            batch_id=_persistence_id)

        marker.children, _ = make_markers_for_tasks(_tasks, group=marker,
            batch_id=_persistence_id)

        task_count = len(_tasks)
        node_count = marker.count_nodes()
        self.assertGreater(node_count,task_count)
        self.assertEqual(node_count, tree_graph_growth(task_count))

class TestBuiltTree(unittest.TestCase):
    def setUp(self):
        import uuid

        harness = testbed.Testbed()
        harness.activate()
        harness.init_taskqueue_stub()

        _tasks = [
        Async(target=example_function,
            args=[arg]) for arg in range(0,23)
        ]
        id = uuid.uuid4().hex

        marker = Marker(id=id, group_id=None,
            batch_id=id)

        marker.children, _ = make_markers_for_tasks(_tasks, group=marker,
            batch_id=id)

        self.marker = marker

    def test_leaf_nodes_have_group_id(self):
        group_marker = self.marker.children[0]
        leaf_marker = group_marker.children[3]
        self.assertEqual(group_marker.key,leaf_marker.group_id)



