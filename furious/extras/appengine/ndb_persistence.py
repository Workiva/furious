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


class MarkerPersist(ndb.Model):
    """
    """
    group_id = ndb.StringProperty(indexed=False)
    group = ndb.KeyProperty(indexed=False)
    callbacks = ndb.JsonProperty()
    children = ndb.KeyProperty(repeated=True,indexed=False)
    done = ndb.BooleanProperty(default=False,indexed=False)
    result = ndb.JsonProperty()


    @classmethod
    def from_marker(cls,marker):
        marker_dict = marker.to_dict()
        return cls(
            id=marker.id,
            group_id=marker.group_id,
            done = marker.done,
            result = marker.result,
            group = (ndb.Key('MarkerPersist',marker.group_id)
                     if marker.group_id else None),
            callbacks=marker_dict.get('callbacks'),
            children = [ndb.Key('MarkerPersist',id) for id in
                        marker.children_as_ids()]
        )

    def to_marker(self):
        from furious.context.completion_marker import Marker
        return Marker(
            id=(self.key.id() if self.key else None),
            group_id=self.group_id,
            done=self.done,
            result=self.result,
            callbacks=self.callbacks,
            children=[key.id() for key in self.children
                      if key]
        )



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
        if isinstance(child,Marker):
            child_mp, child_futures = _marker_persist(child, save_leaves)
            if child_mp:
                persisted_children_keys.append(child_mp.key)
            if child_futures:
                put_futures.extend(child_futures)

    if save_leaves or not marker.is_leaf():
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
    logging.info("all root and internal vertex markers saved")
    return


def marker_get(id):
    key = ndb.Key('MarkerPersist',id)
    mp = key.get()
    if mp:
        return mp.to_marker()


def marker_get_multi(ids):
    keys = [ndb.Key('MarkerPersist',id) for id in ids]
    mps = ndb.get_multi(keys)
    return [mp.to_marker() for mp in mps if mp]


def marker_get_children(marker):
    mp = MarkerPersist.from_marker(marker)
    children = ndb.get_multi(mp.children)
    return [child.to_marker() for child in children if child]
