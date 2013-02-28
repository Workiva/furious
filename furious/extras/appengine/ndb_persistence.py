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
from google.appengine.ext.ndb import Future
from google.appengine.ext import ndb
logger = logging.getLogger('marker_tree')


class ContextPersist(ndb.Model):
    data = ndb.JsonProperty()
    created = ndb.DateTimeProperty(auto_now_add=True)


class MarkerResult(ndb.Model):
    """Represents the result portion of the Async job results

    """
    result = ndb.JsonProperty()


class MarkerPersist(ndb.Model):
    """
    """
    group_id = ndb.StringProperty(indexed=False)
    group = ndb.KeyProperty(indexed=False)
    callbacks = ndb.JsonProperty()
    children = ndb.KeyProperty(repeated=True, indexed=False)
    done = ndb.BooleanProperty(default=False, indexed=False)
    result = ndb.JsonProperty()
    created = ndb.DateTimeProperty(auto_now_add=True, indexed=False)
    updated = ndb.DateTimeProperty(auto_now=True, indexed=False)

    @classmethod
    def from_marker(cls, marker):
        marker_dict = marker.to_dict()
        if hasattr(marker, '_marker_persist'):
            referenced_marker_persist = marker._marker_persist
            referenced_marker_persist.done = marker.done
            referenced_marker_persist.result = marker.result
            referenced_marker_persist.group = (
                ndb.Key('MarkerPersist', marker.group_id)
                if marker.group_id else None)
            referenced_marker_persist.callbacks = marker_dict.get('callbacks')
            referenced_marker_persist.children = [
                ndb.Key('MarkerPersist', idx) for idx in
                marker.children_as_ids()
            ]
            return referenced_marker_persist

        return cls(
            id=marker.id,
            group_id=marker.group_id,
            done=marker.done,
            result=marker.result,
            group=(ndb.Key('MarkerPersist', marker.group_id)
                   if marker.group_id else None),
            callbacks=marker_dict.get('callbacks'),
            children=[ndb.Key('MarkerPersist', idx) for idx in
                      marker.children_as_ids()]
        )

    def to_marker(self):
        from furious.marker_tree.marker import Marker
        children_ids = []
        for key in self.children:
            if key:
                children_ids.append(key.id())

        created_time = round(float(self.created.strftime('%s.%f')), 3)
        last_updated_time = round(float(self.updated.strftime('%s.%f')), 3)

        work_time = last_updated_time - created_time
        key_id = (self.key.id() if self.key else None)

        marker = Marker(
            id=key_id,
            group_id=self.group_id,
            done=self.done,
            work_time=work_time,
            result=self.result,
            callbacks=self.callbacks,
            children=children_ids
        )
        marker._marker_persist = self

        return marker


def _marker_persist(marker, save_leaves=True):
    """
    ndb Marker persist strategy
    _marker_persist is recursive, persisting all child markers
    asynchronously. It collects the put futures as it goes.
    marker_persist waits for the put futures to finish.
    """
    from furious.marker_tree.marker import Marker

    mp = MarkerPersist.from_marker(marker)
    put_futures = []

    persisted_children_keys = []
    for child in marker.children:
        if isinstance(child, Marker):
            child_mp, child_futures = _marker_persist(child, save_leaves)
            if child_mp:
                persisted_children_keys.append(child_mp.key)
            if child_futures:
                put_futures.extend(child_futures)

    logger.debug("save marker? leaf:%s  %s group_id: %s" % (
        marker.is_leaf(), marker.id, marker.get_group_id()))
    if save_leaves or not marker.is_leaf():
        logger.debug("saved marker %s" % marker.id)

        # Remove the result property from marker because
        # the result is not to be stored in the MarkerPersist
        # entity.
        result = mp.result
        mp.result = None

        put_future = mp.put_async()
        put_futures.append(put_future)

        # Persist possible result in a separate entity.
        if result:
            result_entity = MarkerResult(id=marker.id, result=result)
            result_put_future = result_entity.put_async()
            put_futures.append(result_put_future)

    return mp, put_futures


def marker_persist(marker, save_leaves=True):
    """
    ndb marker persist strategy
    this is called by a root marker
    """
    mp, put_futures = _marker_persist(marker, save_leaves)
    Future.wait_all(put_futures)
    logging.debug("marker saved id: %s" % marker.id)
    return


def marker_get(idx, load_results=False):
    key = ndb.Key('MarkerPersist', idx)

    if load_results:
        result_key = ndb.Key('MarkerResult', idx)
        marker_persisted, result = ndb.get_multi([key, result_key])
        if result:
            marker_persisted.result = result.result
    else:
        marker_persisted = key.get()

    if marker_persisted:
        return marker_persisted.to_marker()


def marker_get_multi(ids, load_results=False):
    keys = [ndb.Key('MarkerPersist', idx) for idx in ids]
    marker_futures = ndb.get_multi_async(keys)

    results_persisted = {}
    marker_result_futures = None
    if load_results:
        result_keys = [ndb.Key('MarkerResult', idx) for idx in ids]
        marker_result_futures = ndb.get_multi_async(result_keys)

    if marker_result_futures:
        Future.wait_all(marker_result_futures)
        for future in marker_result_futures:
            result = future.get_result()
            if result:
                results_persisted[result.key.id()] = result

    Future.wait_all(marker_futures)

    markers = []
    for future in marker_futures:
        marker_persisted = future.get_result()
        if marker_persisted:
            result = results_persisted.get(marker_persisted.key.id())
            if result:
                marker_persisted.result = result.result
            markers.append(marker_persisted.to_marker())

    return markers


def marker_get_children(marker, load_results=False):
    children = marker_get_multi(
        marker.children_as_ids(), load_results=load_results)
    return children


def store_context(idx, context_dict):
    cp = ContextPersist(id=idx, data=context_dict)
    cp.put()


def load_context(idx):
    cp = ContextPersist.get_by_id(idx)
    return cp.data
