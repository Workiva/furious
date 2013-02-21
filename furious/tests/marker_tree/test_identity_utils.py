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


class TestFunctions(unittest.TestCase):
    def test_leaf_id_functions(self):
        """
        Ensure the group id can be retrieved from the id given to an async
        """
        import uuid
        from furious.marker_tree.exceptions import InvalidLeafId
        from furious.marker_tree.identity_utils import leaf_persistence_id_from_group_id
        from furious.marker_tree.identity_utils import leaf_persistence_id_to_group_id

        group_id = uuid.uuid4().hex
        index = 2
        leaf_key = leaf_persistence_id_from_group_id(group_id, index)
        self.assertIsNotNone(leaf_key)
        self.assertEqual(len(leaf_key.split(',')), 2)
        reconstituted_group_id = leaf_persistence_id_to_group_id(leaf_key)
        self.assertEqual(reconstituted_group_id, group_id)

        group_id = "{0},2,4".format(uuid.uuid4().hex)
        index = 2
        leaf_key = leaf_persistence_id_from_group_id(group_id, index)
        self.assertIsNotNone(leaf_key)
        self.assertGreaterEqual(len(leaf_key.split(',')), 2)
        reconstituted_group_id = leaf_persistence_id_to_group_id(leaf_key)
        self.assertEqual(reconstituted_group_id, group_id)

        def wrapper():
            return leaf_persistence_id_to_group_id("2")

        self.assertRaises(InvalidLeafId, wrapper)

        def non_string_wrapper():
            return leaf_persistence_id_to_group_id(2)

        self.assertRaises(InvalidLeafId, non_string_wrapper)

        group_id = leaf_persistence_id_to_group_id(u"234,4")
        self.assertEqual(u"234", group_id)

    def test_random_string_generator(self):
        """
        Make sure random alpha numeric string generator
        produces a string of the expected length with only
        chars of A-z0-9
        """
        from furious.marker_tree.identity_utils import random_alpha_numeric

        value = random_alpha_numeric()
        self.assertIsInstance(value, basestring)
        self.assertEqual(len(value), 2)
        self.assertRegexpMatches(value, r"^[A-Za-z0-9]{2}$")

    def test_ordered_random_task_ids(self):
        """
        Make sure an ordered set list of random strings are returned
        where the length is number_of_ids
        """
        from furious.marker_tree.identity_utils import ordered_random_ids

        ids = ordered_random_ids(10)
        self.assertEqual(len(ids), 10)
        for index, item in enumerate(ids):
            self.assertIsNotNone(item)
            self.assertIsInstance(item, basestring)
            self.assertEqual(len(item), 3)
            self.assertEqual(str(index), item[-1])
        sorted_ids = sorted(ids)
        self.assertListEqual(ids, sorted_ids)
