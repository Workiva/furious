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
import time
logger = logging.getLogger('completion_marker')


class ContextPersist(ndb.Model):
    data = ndb.JsonProperty()
    created = ndb.DateTimeProperty(auto_now_add=True)


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
        if hasattr(marker,'_marker_persist'):
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
        from furious.context.completion_marker import Marker
        children_ids = []
        for key in self.children:
            if key:
                children_ids.append(key.id())

        created_time = round(float(self.created.strftime('%s.%f')), 3)
        last_updated_time = round(float(self.updated.strftime('%s.%f')), 3)

        work_time = last_updated_time - created_time

        marker = Marker(
            id=(self.key.id() if self.key else None),
            group_id=self.group_id,
            done=self.done,
            work_time = work_time,
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
    from furious.context.completion_marker import Marker

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
        put_future = mp.put_async()
        put_futures.append(put_future)

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


def marker_get(idx):
    key = ndb.Key('MarkerPersist', idx)
    mp = key.get()
    if mp:
        return mp.to_marker()


def marker_get_multi(ids):
    keys = [ndb.Key('MarkerPersist', idx) for idx in ids]
    mps = ndb.get_multi(keys)
    return [mp.to_marker() for mp in mps if mp]


def marker_get_children(marker):
    mp = MarkerPersist.from_marker(marker)
    children = ndb.get_multi(mp.children)
    return [child.to_marker() for child in children if child]


def store_context(idx, context_dict):
    cp = ContextPersist(id=idx, data=context_dict)
    cp.put()


def load_context(idx):
    cp = ContextPersist.get_by_id(idx)
    return cp.data
