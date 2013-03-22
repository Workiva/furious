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

from google.appengine.ext import testbed
from mock import patch


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
        from furious.marker_tree.marker import Marker
        from furious.marker_tree.graph_analysis import tree_graph_growth

        from furious.tests.marker_tree import dummy_success_callback

        context = Context(group_size=1)

        for arg in xrange(23):
            context.add(target=dummy_success_callback,
                        args=[arg])

        _local.get_local_context().registry.append(context)

        root_marker = Marker.make_marker_tree_for_context(context)

        # Plus one, because of the empty context.
        self.assertEqual(root_marker.count_nodes(), tree_graph_growth(23))

    def test_non_default_group_size(self):
        from furious.context import _local
        from furious.context.context import Context
        from furious.marker_tree.marker import Marker

        from furious.tests.marker_tree import dummy_success_callback

        context = Context(group_size=11, batch_size=5)

        for arg in xrange(55):
            context.add(target=dummy_success_callback,
                        args=[arg])

        _local.get_local_context().registry.append(context)

        root_marker = Marker.make_marker_tree_for_context(context)

        self.assertEqual(len(root_marker.children), 11)

    def test_count_nodes(self):
        """Ensure Marker.count_nodes

        """
        from furious.context import _local
        from furious.context.context import Context
        from furious.marker_tree.marker import Marker

        from furious.tests.marker_tree import dummy_success_callback

        context = Context(group_size=1)

        context.add(target=dummy_success_callback,
                    args=[1])

        _local.get_local_context().registry.append(context)

        root_marker = Marker.make_marker_tree_for_context(context)

        root_marker.persist()
        # Plus one, because of the empty context.
        self.assertEqual(root_marker.count_nodes(), 2)

    @patch('furious.marker_tree.marker.Marker.update_done', auto_spec=True)
    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_handle_async_done(self, queue_add_mock, update_done_mock):
        """Ensure handle_async_done is called when Context is given a
        callback.
        """
        from furious.context import Context
        from furious.context._execution import _ExecutionContext
        from furious.processors import run_job

        update_done_mock.return_value = True

        with Context(group_size=1, callbacks={
                'success': example_callback_success}) as ctx:
            job = ctx.add(example_function, args=[1, 2])

        with _ExecutionContext(job):
            run_job()

        update_done_mock.assert_called_once()
        queue_add_mock.assert_called_once()

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_internal_vertex_combiner_called(self, queue_add_mock):
        """Call lines_combiner and small_aggregated_results_success_callback
        """
        from furious.context import Context
        from furious.context._execution import _ExecutionContext
        from furious.processors import run_job
        from furious.extras.combiners import lines_combiner
        from furious.extras.callbacks import small_aggregated_results_success_callback

        with Context(callbacks={
                'internal_vertex_combiner': lines_combiner,
                'success': small_aggregated_results_success_callback}) as ctx:
            job = ctx.add(pass_args_function, args=[1, 2])

        with _ExecutionContext(job):
            run_job()

        queue_add_mock.assert_called_once()

    @patch('furious.extras.combiners.lines_combiner', auto_spec=True)
    @patch('furious.extras.callbacks.small_aggregated_results_success_callback', auto_spec=True)
    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_success_and_combiner_called(self, queue_add_mock,
                                         success_mock,
                                         combiner_mock):
        """Ensure context success and internal vertex combiner
        is called when all the context's tasks are processed.
        """
        from furious.context import Context
        from furious.context._execution import _ExecutionContext
        from furious.processors import run_job
        from furious.job_utils import encode_callbacks

        with Context(callbacks=encode_callbacks(
                {'internal_vertex_combiner':
                     'furious.extras.combiners.lines_combiner',
                    'success':
                    'furious.extras.callbacks.small_aggregated'
                    '_results_success_callback'})) as ctx:
            job = ctx.add(pass_args_function, args=[1, 2])

        with _ExecutionContext(job):
            run_job()

        queue_add_mock.assert_called_once()
        combiner_mock.assert_called_once()
        success_mock.assert_called_once()


def l_combiner(results):
    return reduce(lambda x, y: x + y, results, 0)


def iv_combiner(results):
    return results


def example_callback_success(idx, result):
    return [idx, result]


def example_function(*args, **kwargs):
    """This function is called by furious tasks to demonstrate usage."""
    logging.info('example_function executed with args: %r, kwargs: %r',
                 args, kwargs)
    return l_combiner(args)


def pass_args_function(*args, **kwargs):
    if kwargs:
        pass
    return args
