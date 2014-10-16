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
import json
import os
import unittest

from google.appengine.ext import testbed
from google.appengine.datastore import datastore_stub_util
from google.appengine.runtime.apiproxy_errors import DeadlineExceededError

from mock import Mock
from mock import patch

from furious.async import Async
from furious.async import AsyncResult

from furious.context import Context

from furious.processors import encode_exception

from furious.extras.appengine.ndb_persistence import context_completion_checker
from furious.extras.appengine.ndb_persistence import ContextResult
from furious.extras.appengine.ndb_persistence import _completion_checker
from furious.extras.appengine.ndb_persistence import FuriousAsyncMarker
from furious.extras.appengine.ndb_persistence import FuriousContext
from furious.extras.appengine.ndb_persistence import FuriousCompletionMarker
from furious.extras.appengine.ndb_persistence import iter_context_results
from furious.extras.appengine.ndb_persistence import store_async_marker
from furious.extras.appengine.ndb_persistence import store_async_result
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


class QueueTestCase(NdbTestBase):

    def test_queue_assignment(self):
        from furious.extras.appengine.ndb_persistence import _get_current_queue

        queue = "test"
        self.testbed.setup_env(HTTP_X_APPENGINE_QUEUENAME=queue)
        result = _get_current_queue()
        self.assertEqual(result, queue)


class ContextCompletionCheckerTestCase(NdbTestBase):

    def test_completion_store(self):
        """Ensure the marker is stored on completion even without a result."""

        async = Async('foo')
        async._executed = True

        result = context_completion_checker(async)

        self.assertTrue(result)

        marker = FuriousAsyncMarker.get_by_id(async.id)

        self.assertIsNotNone(marker)
        self.assertEqual(marker.key.id(), async.id)
        self.assertEqual(marker.status, -1)

    def test_completion_store_with_result(self):
        """Ensure the marker is stored on completion with a result."""

        async = Async('foo')
        async._executing = True
        async.result = AsyncResult(status=1)
        async._executed = True

        result = context_completion_checker(async)

        self.assertTrue(result)

        marker = FuriousAsyncMarker.get_by_id(async.id)

        self.assertIsNotNone(marker)
        self.assertEqual(marker.key.id(), async.id)
        self.assertEqual(marker.status, 1)


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

        store_async_marker(async_id, 0)

        self.assertIsNotNone(FuriousAsyncMarker.get_by_id(async_id))

    def test_marker_does_exist(self):
        """Ensure the marker is not saved if it already exists."""
        async_id = "asyncid"
        result = '{"foo": "bar"}'
        FuriousAsyncMarker(id=async_id, result=result, status=1).put()

        store_async_marker(async_id, 1)

        marker = FuriousAsyncMarker.get_by_id(async_id)

        self.assertEqual(marker.result, result)
        self.assertEqual(marker.status, 1)

    def test_store_async_exception(self):
        """Ensure an async exception is encoded correctly."""
        async_id = "asyncid"
        async_result = AsyncResult()
        try:
            raise Exception()
        except Exception, e:
            async_result.payload = encode_exception(e)
            async_result.status = async_result.ERROR

        store_async_result(async_id, async_result)

        marker = FuriousAsyncMarker.get_by_id(async_id)

        self.assertEqual(marker.result, json.dumps(async_result.to_dict()))
        self.assertEqual(marker.status, async_result.ERROR)


@patch('furious.extras.appengine.ndb_persistence._check_markers')
@patch.object(FuriousContext, 'from_id')
class CompletionCheckerTestCase(NdbTestBase):

    def test_markers_not_complete(self, context_from_id, check_markers):
        """Ensure if not all markers are complete that False is returned and
        the completion handler and cleanup tasks are not triggered.
        """
        complete_event = Mock()
        context = Context(id="contextid",
                          callbacks={'complete': complete_event})

        context_from_id.return_value = context

        check_markers.return_value = False, False

        async = Async('foo')
        async.update_options(context_id='contextid')

        result = _completion_checker(async.id, async.context_id)

        self.assertFalse(result)

        self.assertTrue(context_from_id.called)

        self.assertFalse(complete_event.start.called)

    def test_no_context_id(self, context_from_id, check_markers):
        """Ensure if no context id that nothing happens.
        """
        result = _completion_checker("1", None)

        self.assertIsNone(result)

        self.assertFalse(context_from_id.called)

        self.assertFalse(check_markers.start.called)

    def test_markers_complete(self, context_from_id, check_markers):
        """Ensure if all markers are complete that True is returned and the
        completion handler and cleanup tasks are triggered.
        """
        complete_event = Mock()
        context = Context(id="contextid",
                          callbacks={'complete': complete_event})

        context_from_id.return_value = context

        check_markers.return_value = True, False

        async = Async('foo')
        async.update_options(context_id='contextid')
        FuriousCompletionMarker(id=async.context_id, complete=False).put()

        result = _completion_checker(async.id, async.context_id)

        self.assertTrue(result)

        complete_event.start.assert_called_once_with(transactional=True)

    @patch('furious.extras.appengine.ndb_persistence._mark_context_complete')
    def test_markers_and_context_complete(self, mark, context_from_id,
                                          check_markers):
        """Ensure if all markers are complete that True is returned and
        nothing else is done.
        """
        async = Async('foo')
        async.update_options(context_id='contextid')

        complete_event = Mock()
        context = Context(id="contextid",
                          callbacks={'complete': complete_event})

        context_from_id.return_value = context

        marker = FuriousCompletionMarker(id="contextid", complete=True)
        marker.put()

        check_markers.return_value = True, False
        mark.return_value = True

        result = _completion_checker(async.id, async.context_id)

        self.assertTrue(result)

        self.assertFalse(complete_event.start.called)

        marker.key.delete()

    @patch('furious.extras.appengine.ndb_persistence._insert_post_complete_tasks')
    def test_marker_not_complete_when_start_fails(self, mock_insert,
                                                  context_from_id,
                                                  check_markers):
        """Ensure if the completion handler fails to start, that the marker
        does not get marked as complete.
        """

        complete_event = Mock()
        context = Context(id="contextid",
                          callbacks={'complete': complete_event})

        context_from_id.return_value = context

        check_markers.return_value = True, False

        async = Async('foo')
        async.update_options(context_id='contextid')
        FuriousCompletionMarker(id=async.context_id, complete=False).put()

        # Simulate the task failing to start
        mock_insert.side_effect = DeadlineExceededError()

        self.assertRaises(DeadlineExceededError,
                          _completion_checker, async.id, async.context_id)

        # Marker should not have been marked complete.
        current_marker = FuriousCompletionMarker.get_by_id(async.context_id)
        self.assertFalse(current_marker.complete)


@patch('furious.extras.appengine.ndb_persistence.ndb.get_multi')
class CheckMarkersTestCase(NdbTestBase):

    def test_all_markers_exist(self, get_multi):
        """Ensure True is returned when all markers exist."""
        task_ids = map(lambda x: "task" + str(x), range(11))

        get_multi.side_effect = [Mock()], [Mock()]

        done, has_errors = _check_markers(task_ids)

        self.assertTrue(done)
        self.assertFalse(has_errors)

    def test_not_all_markers_exist(self, get_multi):
        """Ensure False is returned when not all markers exist."""
        task_ids = map(lambda x: "task" + str(x), range(11))

        get_multi.side_effect = [Mock()], [None]

        done, has_errors = _check_markers(task_ids)

        self.assertFalse(done)
        self.assertFalse(has_errors)


@patch('furious.extras.appengine.ndb_persistence.ndb.get_multi_async')
class IterResultsTestCase(NdbTestBase):

    def test_more_results_than_batch_size(self, get_multi_async):
        """Ensure all the results are yielded out when more than the batch
        size.
        """
        marker1 = _build_marker(payload="1", status=1)
        marker2 = _build_marker(payload="2", status=1)
        marker3 = _build_marker(payload="3", status=1)

        future_set_1 = [_build_future(marker1),
                        _build_future(marker2)]
        future_set_2 = [_build_future(marker3)]

        get_multi_async.side_effect = future_set_1, future_set_2

        context = Context(_task_ids=["1", "2", "3"])

        results = list(iter_context_results(context, batch_size=2))

        self.assertEqual(results[0], ("1", marker1))
        self.assertEqual(results[1], ("2", marker2))
        self.assertEqual(results[2], ("3", marker3))

    def test_less_results_than_batch_size(self, get_multi_async):
        """Ensure all the results are yielded out when less than the batch
        size.
        """
        marker1 = _build_marker(payload="1", status=1)
        marker2 = _build_marker(payload="2", status=1)
        marker3 = _build_marker(payload="3", status=1)

        future_set_1 = [_build_future(marker1), _build_future(marker2),
                        _build_future(marker3)]

        get_multi_async.return_value = future_set_1

        context = Context(_task_ids=["1", "2", "3"])

        results = list(iter_context_results(context))

        self.assertEqual(results[0], ("1", marker1))
        self.assertEqual(results[1], ("2", marker2))
        self.assertEqual(results[2], ("3", marker3))

    def test_no_task_ids(self, get_multi_async):
        """Ensure no results are yielded out when there are no task ids on the
        passed in context.
        """
        get_multi_async.return_value = []
        context = Context(_task_ids=[])

        results = list(iter_context_results(context))

        self.assertEqual(results, [])

    def test_keys_with_no_results(self, get_multi_async):
        """Ensure empty results are yielded out when there are no items to
        load but task ids are on the passed in context.
        """
        future_set_1 = [_build_future(), _build_future(), _build_future()]

        get_multi_async.return_value = future_set_1

        context = Context(_task_ids=["1", "2", "3"])

        results = list(iter_context_results(context))

        self.assertEqual(results[0], ("1", None))
        self.assertEqual(results[1], ("2", None))
        self.assertEqual(results[2], ("3", None))

    def test_failure_in_marker(self, get_multi_async):
        """Ensure all the results are yielded out when less than the batch
        size and a failure is included in the results.
        """
        async_id = "1"
        async_result = AsyncResult()
        try:
            raise Exception()
        except Exception, e:
            async_result.payload = encode_exception(e)
            async_result.status = async_result.ERROR

        json_dump = json.dumps(async_result.to_dict())
        marker1 = FuriousAsyncMarker(id=async_id, result=json_dump,
                                     status=async_result.status)

        marker2 = FuriousAsyncMarker(
            result=json.dumps(AsyncResult(payload="2", status=1).to_dict()))
        marker3 = FuriousAsyncMarker(
            result=json.dumps(AsyncResult(payload="3", status=1).to_dict()))

        future_set_1 = [_build_future(marker1), _build_future(marker2),
                        _build_future(marker3)]

        get_multi_async.return_value = future_set_1

        context = Context(_task_ids=["1", "2", "3"])
        context_result = ContextResult(context)

        results = list(context_result.items())

        self.assertEqual(results[0], ("1", json.loads(json_dump)["payload"]))
        self.assertEqual(results[1], ("2", "2"))
        self.assertEqual(results[2], ("3", "3"))


class ContextResultTestCase(NdbTestBase):

    @patch('furious.extras.appengine.ndb_persistence.ndb.get_multi_async')
    def test_results_with_no_tasks_loaded(self, get_multi_async):
        """Ensure results loads the tasks and yields them out when no tasks are
        cached.
        """
        marker1 = _build_marker(payload="1", status=1)
        marker2 = _build_marker(payload="2", status=1)
        marker3 = _build_marker(payload="3", status=1)

        future_set_1 = [_build_future(marker1), _build_future(marker2),
                        _build_future(marker3)]

        get_multi_async.return_value = future_set_1

        context = Context(_task_ids=["1", "2", "3"])
        context_result = ContextResult(context)

        results = list(context_result.items())

        results = sorted(results)

        self.assertEqual(results, [("1", "1"), ("2", "2"), ("3", "3")])

        self.assertEqual(context_result._task_cache, {
            "1": marker1,
            "2": marker2,
            "3": marker3
        })

    @patch('furious.extras.appengine.ndb_persistence.ndb.get_multi_async')
    def test_results_with_tasks_loaded(self, get_multi_async):
        """Ensure results uses the cached tasks and yields them out when tasks
        are cached.
        """
        marker1 = _build_marker(payload="1", status=1)
        marker2 = _build_marker(payload="2", status=1)
        marker3 = _build_marker(payload="3", status=1)

        context = Context(_task_ids=["1", "2", "3"])
        context_result = ContextResult(context)

        context_result._task_cache = {
            "1": marker1,
            "2": marker2,
            "3": marker3
        }

        results = list(context_result.items())

        results = sorted(results)

        self.assertEqual(results, [("1", "1"), ("2", "2"), ("3", "3")])

        self.assertFalse(get_multi_async.called)

    @patch('furious.extras.appengine.ndb_persistence.ndb.get_multi_async')
    def test_results_with_tasks_loaded_missing_result(self, get_multi_async):
        """Ensure results uses the cached tasks and yields them out when tasks
        are cached and there's no results.
        """
        marker1 = FuriousAsyncMarker()

        context = Context(_task_ids=["1", "2", "3"])
        context_result = ContextResult(context)

        context_result._task_cache = {
            "1": marker1,
            "2": None,
            "3": None
        }

        results = list(context_result.items())

        results = sorted(results)

        self.assertEqual(results, [("1", None), ("2", None), ("3", None)])

        self.assertFalse(get_multi_async.called)

    def test_has_errors_with_marker_not_cached(self):
        """Ensure returns the value from the marker when not cached."""
        context_id = 1

        FuriousCompletionMarker(id=context_id, has_errors=True).put()

        context = Context(id=context_id)
        context_result = ContextResult(context)

        self.assertIsNone(context_result._marker)
        self.assertTrue(context_result.has_errors())

        context_result._marker.key.delete()

    def test_has_errors_with_marker_cached(self):
        """Ensure returns the value from the marker when cached."""
        context_id = 1

        marker = FuriousCompletionMarker(id=context_id, has_errors=True)
        marker.put()

        context = Context(id=context_id)
        context._marker = marker
        context_result = ContextResult(context)

        self.assertIsNone(context_result._marker)
        self.assertTrue(context_result.has_errors())

        marker.key.delete()

    def test_has_no_marker(self):
        """Ensure returns False when no marker found."""
        context_id = 1

        context = Context(id=context_id)
        context_result = ContextResult(context)

        self.assertIsNone(context_result._marker)
        self.assertFalse(context_result.has_errors())


def _build_marker(payload=None, status=None):
    return FuriousAsyncMarker(result=json.dumps(
        {
            'payload': payload,
            'status': status
        }
    ))


def _build_future(result=None):
    future = Mock()

    if not result:
        future.get_result.return_value = None
    else:
        future.get_result.return_value = result

    return future
