#
# Copyright 2014 WebFilings, LLC
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
import os
import unittest

from google.appengine.ext import testbed
from google.appengine.datastore import datastore_stub_util

from mock import Mock
from mock import patch

from furious.async import Async
from furious.context import Context

from furious.extras.appengine.ndb_persistence import context_completion_checker
from furious.extras.appengine.ndb_persistence import FuriousAsyncMarker
from furious.extras.appengine.ndb_persistence import FuriousContext
from furious.extras.appengine.ndb_persistence import iter_results
from furious.extras.appengine.ndb_persistence import store_async_marker
from furious.extras.appengine.ndb_persistence import store_context
from furious.extras.appengine.ndb_persistence import _check_markers


HRD_POLICY_PROBABILITY = 1


class NdbTestBase(unittest.TestCase):

    def setUp(self):
        super(NdbTestBase, self).setUp()

        os.environ['TZ'] = "UTC"

        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.setup_env(app_id="furious")

        self.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
            probability=HRD_POLICY_PROBABILITY)
        self.testbed.init_datastore_v3_stub(consistency_policy=self.policy)
        self.testbed.init_memcache_stub()

        # TODO: Kill this
        marker = FuriousAsyncMarker.query().fetch(1)
        self.assertEqual(marker, [])

    def tearDown(self):
        self.testbed.deactivate()

        super(NdbTestBase, self).tearDown()


@patch('furious.extras.appengine.ndb_persistence._check_markers')
@patch.object(FuriousContext, 'from_id')
class ContextCompletionCheckerTestCase(NdbTestBase):

    def test_markers_not_complete(self, context_from_id, check_markers):
        """Ensure if not all markers are complete that False is returned and
        the completion handler and cleanup tasks are not triggered.
        """
        complete_event = Mock()
        context = Context(id="contextid",
                          callbacks={'complete': complete_event})

        context_from_id.return_value = context

        check_markers.return_value = False

        async = Async('foo')

        result = context_completion_checker(async)

        self.assertFalse(result)

        self.assertFalse(complete_event.start.called)

        marker = FuriousAsyncMarker.get_by_id(async.id)

        self.assertIsNotNone(marker)
        self.assertEqual(marker.key.id(), async.id)

    def test_markers_complete(self, context_from_id, check_markers):
        """Ensure if all markers are complete that True is returned and the
        completion handler and cleanup tasks are triggered.
        """
        complete_event = Mock()
        context = Context(id="contextid",
                          callbacks={'complete': complete_event})

        context_from_id.return_value = context

        check_markers.return_value = True

        async = Async('foo')

        result = context_completion_checker(async)

        self.assertTrue(result)

        complete_event.start.assert_called_once_with()

        marker = FuriousAsyncMarker.get_by_id(async.id)

        self.assertIsNotNone(marker)
        self.assertEqual(marker.key.id(), async.id)


@patch('furious.extras.appengine.ndb_persistence.ndb.get_multi')
class CheckMarkersTestCase(NdbTestBase):

    def test_all_markers_exist(self, get_multi):
        """Ensure True is returned when all markers exist."""
        task_ids = map(lambda x: "task" + str(x), range(11))

        get_multi.side_effect = [Mock()], [Mock()]

        result = _check_markers(task_ids)

        self.assertTrue(result)

    def test_not_all_markers_exist(self, get_multi):
        """Ensure False is returned when not all markers exist."""
        task_ids = map(lambda x: "task" + str(x), range(11))

        get_multi.side_effect = [Mock()], [None]

        result = _check_markers(task_ids)

        self.assertFalse(result)


class StoreContextTestCase(NdbTestBase):

    def test_save_context(self):
        """Ensure the passed in context gets serialized and set on the saved
        FuriousContext entity.
        """
        _id = "contextid"

        context = Context(id=_id)

        result = store_context(context)

        self.assertEqual(result.id(), _id)

        loaded_context = FuriousContext.from_id(result.id())

        self.assertEqual(context.to_dict(), loaded_context.to_dict())


class StoreAsyncMarkerTestCase(NdbTestBase):

    def test_marker_does_not_exist(self):
        """Ensure the marker is saved if it does not already exist."""
        async_id = "asyncid"

        store_async_marker(async_id)

        self.assertIsNotNone(FuriousAsyncMarker.get_by_id(async_id))

    def test_marker_does_exist(self):
        """Ensure the marker is not saved if it already exists."""
        async_id = "asyncid"
        result = '{"foo": "bar"}'
        FuriousAsyncMarker(id=async_id, result=result).put()

        store_async_marker(async_id)

        marker = FuriousAsyncMarker.get_by_id(async_id)

        self.assertEqual(marker.result, result)


@patch('furious.extras.appengine.ndb_persistence.ndb.get_multi_async')
class IterResultsTestCase(NdbTestBase):

    def _build_future(self, result=None):
        future = Mock()

        if not result:
            future.get_result.return_value = None
        else:
            future.get_result.return_value = FuriousAsyncMarker(result=result)

        return future

    def test_more_results_than_batch_size(self, get_multi_async):
        """Ensure all the results are yielded out when more than the batch
        size.
        """
        future_set_1 = [self._build_future("1"), self._build_future("2")]
        future_set_2 = [self._build_future("3")]

        get_multi_async.side_effect = future_set_1, future_set_2

        context = Context(_task_ids=["1", "2", "3"])

        results = list(iter_results(context, batch_size=2))

        self.assertEqual(results[0], ("1", 1))
        self.assertEqual(results[1], ("2", 2))
        self.assertEqual(results[2], ("3", 3))

    def test_less_results_than_batch_size(self, get_multi_async):
        """Ensure all the results are yielded out when less than the batch
        size.
        """
        future_set_1 = [self._build_future("1"), self._build_future("2"),
                        self._build_future("3")]

        get_multi_async.return_value = future_set_1

        context = Context(_task_ids=["1", "2", "3"])

        results = list(iter_results(context))

        self.assertEqual(results[0], ("1", 1))
        self.assertEqual(results[1], ("2", 2))
        self.assertEqual(results[2], ("3", 3))

    def test_no_task_ids(self, get_multi_async):
        """Ensure no results are yielded out when there are no task ids on the
        passed in context.
        """
        get_multi_async.return_value = []
        context = Context(_task_ids=[])

        results = list(iter_results(context))

        self.assertEqual(results, [])

    def test_keys_with_no_results(self, get_multi_async):
        """Ensure empty results are yielded out when there are no items to
        load but task ids are on the passed in context.
        """
        future_set_1 = [self._build_future(), self._build_future(),
                        self._build_future()]

        get_multi_async.return_value = future_set_1

        context = Context(_task_ids=["1", "2", "3"])

        results = list(iter_results(context))

        self.assertEqual(results[0], ("1", None))
        self.assertEqual(results[1], ("2", None))
        self.assertEqual(results[2], ("3", None))
