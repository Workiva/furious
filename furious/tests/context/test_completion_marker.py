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
        from furious.context.completion_marker import tree_graph_growth
        sizes = [tree_graph_growth(n) for n in range(0,100,10)]
        expected = [1, 11, 23, 35, 47, 59, 71, 83, 95, 107]
        self.assertEqual(sizes,expected)

    def test_initial_save_growth(self):
        from furious.context.completion_marker import initial_save_growth
        sizes = [initial_save_growth(n) for n in range(0,100,10)]
        expected = [1, 1, 3, 5, 7, 9, 11, 13, 15, 17]
        self.assertEqual(sizes,expected)

class TestMarker(unittest.TestCase):
    def test_do_any_have_children(self):
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
