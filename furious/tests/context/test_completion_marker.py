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
import unittest
import uuid

from google.appengine.ext import testbed
from mock import patch


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

    def test_tree_graph_growth(self):
        """
        Test function that allows testing of the
        number of nodes in a marker tree graph
        """
        from furious.context.completion_marker import tree_graph_growth

        sizes = [tree_graph_growth(n) for n in range(0, 100, 10)]
        expected = [1, 11, 23, 35, 47, 59, 71, 83, 95, 107]
        self.assertEqual(sizes, expected)

    def test_initial_save_growth(self):
        """
        Test function that allows testing of the number of
        nodes which are saved when persisted
        """
        from furious.context.completion_marker import initial_save_growth

        sizes = [initial_save_growth(n) for n in range(0, 100, 10)]
        expected = [1, 1, 3, 5, 7, 9, 11, 13, 15, 17]
        self.assertEqual(sizes, expected)

    def test_random_string_generator(self):
        """
        Make sure random alpha numeric string generator
        produces a string of the expected length with only
        chars of A-z0-9
        """
        from furious.context.completion_marker import random_alpha_numeric

        value = random_alpha_numeric()
        self.assertIsInstance(value, basestring)
        self.assertEqual(len(value), 2)
        self.assertRegexpMatches(value, r"^[A-Za-z0-9]{2}$")

    def test_ordered_random_task_ids(self):
        """
        Make sure an ordered set list of random strings are returned
        where the length is number_of_ids
        """
        from furious.context.completion_marker import ordered_random_ids

        ids = ordered_random_ids(10)
        self.assertEqual(len(ids), 10)
        for index, item in enumerate(ids):
            self.assertIsNotNone(item)
            self.assertIsInstance(item, basestring)
            self.assertEqual(len(item), 3)
            self.assertEqual(str(index), item[-1])
        sorted_ids = sorted(ids)
        self.assertListEqual(ids, sorted_ids)


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

    def test_is_marker_leaf(self):
        """
        Make sure a marker can report if it is a leaf marker
        or not
        """
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker

        root_marker = Marker(id="polly")
        for x in xrange(3):
            root_marker.children.append(Marker(id=
                                        leaf_persistence_id_from_group_id(
                                            root_marker.id, x)))

        originally_a_leaf_marker = root_marker.children[0]

        sub_tree_marker = Marker(id=originally_a_leaf_marker.id,
                                 children=[Marker(id=
                                 leaf_persistence_id_from_group_id(
                                     originally_a_leaf_marker.id, i))
                                     for i in xrange(3)])

        root_marker.children[0] = sub_tree_marker

        still_leaf_marker = root_marker.children[1]
        now_sub_tree_marker = root_marker.children[0]
        self.assertTrue(still_leaf_marker.is_leaf())
        self.assertFalse(now_sub_tree_marker.is_leaf())

    def test_get_multi(self):
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker

        root_marker = Marker(id="freddy")
        for x in xrange(3):
            root_marker.children.append(Marker(id=
                                        leaf_persistence_id_from_group_id(
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
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker

        root_marker = Marker(id="polly")
        for x in xrange(3):
            root_marker.children.append(Marker(id=
                                        leaf_persistence_id_from_group_id(
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
                            leaf_persistence_id_from_group_id(
                                root_marker.id, x)))

        root_marker.children = [marker.id for marker in children]

        self.assertFalse(Marker.do_any_have_children(children))

        first_child = children[0]
        first_child.children = [leaf_persistence_id_from_group_id(
            first_child.id, x) for x in xrange(10)]

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

        marker = Marker.from_dict({'id': 'test'})
        self.assertEqual(marker.id, 'test')
        marker2 = Marker.from_dict(marker.to_dict())
        self.assertEqual(marker2.to_dict(), marker.to_dict())

        root_marker = Marker(id="fun")
        children = []
        for x in xrange(10):
            children.append(Marker(id=
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
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker

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
        from furious.context.completion_marker import Marker

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
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker

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
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker

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
            root_marker.children.append(
                Marker(id=leaf_persistence_id_from_group_id(
                    root_marker.id, x)))

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
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker
        from furious.context.completion_marker import NotSafeToSave

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

        with patch('furious.tests.context.'
                   'test_completion_marker.'
                   'dummy_success_callback', autospec=True) \
                as mock_success_callback:
            with patch('furious.tests.context.'
                       'test_completion_marker.'
                       'dummy_leaf_combiner', autospec=True) \
                    as mock_leaf_combiner:
                with patch('furious.tests.context.'
                           'test_completion_marker.'
                           'dummy_internal_vertex_combiner', autospec=True) \
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

    def test_ordered_grouping_results(self):
        """
        Ensure results will be combined into groups of
        combined leaf results to maintain the result
        order is the same as the tasks inserted.
        When a single task fans out, it's results
        will pulled in position

        """
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import first_iv_markers
        from furious.context.completion_marker import group_into_internal_vertex_results
        from furious.context.completion_marker import Marker

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
        from furious.context.completion_marker import leaf_persistence_id_from_group_id
        from furious.context.completion_marker import Marker
        #build marker tree
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


def dummy_success_callback(idx, results):
    return


def sub_context_empty_target_example():
    from furious.context import Context
    ctx = Context()
    return ctx


def sub_context_target_example():
    from furious.context import Context
    ctx = Context()
    ctx.add(target=dummy_success_callback, args=[1])
    return ctx


def dummy_leaf_combiner(results):
    logging.debug("dummy_leaf_combiner been called!")
    return [result for result in results]


def dummy_internal_vertex_combiner(results):
    logging.debug("dummy_internal_vertex_combiner been called!")
    return results


class TestMarkerTreeBuilding(unittest.TestCase):
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

    def test_build_tree_from_context(self):
        from furious.context import _local
        from furious.context.context import Context
        from furious.context.completion_marker import Marker
        from furious.context.completion_marker import tree_graph_growth

        context = Context()

        for arg in xrange(23):
            context.add(target=dummy_success_callback,
                        args=[arg])

        _local.get_local_context().registry.append(context)

        root_marker = Marker.make_marker_tree_for_context(context)

        root_marker.persist()
        # Plus one, because of the empty context.
        self.assertEqual(root_marker.count_nodes(), tree_graph_growth(23))

    def test_task_returns_empty_context(self):
        from furious.async import Async
        from furious import context

        # Create a new furious Context.
        with context.new(callbacks={'internal_vertex_combiner': l_combiner,
                                    'leaf_combiner': l_combiner,
                                    'success': example_callback_success}) as ctx:
            # "Manually" instantiate and add an Async object to the Context.
            async_task = Async(
                target=example_function, kwargs={'first': 'async'})
            ctx.add(async_task)
            logging.info('Added manual job to context.')

            # instantiate and add an Async who's function creates another Context.
            # enabling extra fan-out of a job
            async_task = Async(
                target=make_a_new_context_example, kwargs={'extra': 'async'})
            ctx.add(async_task)
            logging.info('Added sub context')
            async_task = Async(
                target=make_a_new_empty_context_example, kwargs={'extra': 'async'})
            ctx.add(async_task)
            logging.info('Added sub context')

            # Use the shorthand style, note that add returns the Async object.
            for i in xrange(25):
                ctx.add(target=example_function, args=[i])
                logging.info('Added job %d to context.', i)

            # Instantiate and add an Async who's function creates another Async
            # enabling portions of the job to be serial
            async_task = Async(
                target=make_a_new_async_example, kwargs={'second': 'async'})
            ctx.add(async_task)


def l_combiner(results):
    return reduce(lambda x, y: x + y, results, 0)


def iv_combiner(results):
    return results


def example_callback_success(idx, result):
    pass


def example_function(*args, **kwargs):
    """This function is called by furious tasks to demonstrate usage."""
    logging.info('example_function executed with args: %r, kwargs: %r',
                 args, kwargs)
    return l_combiner(args)


def make_a_new_async_example(*args, **kwargs):
    from furious.async import Async

    async_task = Async(
        target=example_function, args=[500])

    return async_task


def make_a_new_context_example(*args, **kwargs):
    from furious import context

    ctx = context.Context(callbacks={'internal_vertex_combiner': l_combiner,
                                     'leaf_combiner': l_combiner,
                                     'success': example_callback_success})
    # Use the shorthand style, note that add returns the Async object.
    for i in xrange(2):
        ctx.add(target=example_function, args=[i])
        logging.info('Added job %d to context.', i)

    return ctx


def make_a_new_empty_context_example(*args, **kwargs):
    from furious import context

    ctx = context.Context(callbacks={'internal_vertex_combiner': l_combiner,
                                     'leaf_combiner': l_combiner,
                                     'success': example_callback_success})

    return ctx