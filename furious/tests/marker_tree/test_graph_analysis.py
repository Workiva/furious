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
    def test_tree_graph_growth(self):
        """
        Test function that allows testing of the
        number of nodes in a marker tree graph
        """
        from furious.marker_tree.graph_analysis import tree_graph_growth

        sizes = [tree_graph_growth(n) for n in range(0, 100, 10)]
        expected = [1, 11, 23, 35, 47, 59, 71, 83, 95, 107]
        self.assertEqual(sizes, expected)

    def test_initial_save_growth(self):
        """
        Test function that allows testing of the number of
        nodes which are saved when persisted
        """
        from furious.marker_tree.graph_analysis import initial_save_growth

        sizes = [initial_save_growth(n) for n in range(0, 100, 10)]
        expected = [1, 1, 3, 5, 7, 9, 11, 13, 15, 17]
        self.assertEqual(sizes, expected)
