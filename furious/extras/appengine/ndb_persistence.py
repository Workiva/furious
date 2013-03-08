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
    """Used as the model to store a Context in the
    app engine datastore using the ndb api.
    """
    data = ndb.JsonProperty()
    created = ndb.DateTimeProperty(auto_now_add=True)


class MarkerResult(ndb.Model):
    """Represents the result portion of the Async job results
    """
    result = ndb.JsonProperty()


class MarkerPersist(ndb.Model):
    """Used to persist a Marker entity into the app
    engine datastore.
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
        """Instantiate a MarkerPersist directly from a
        Marker object. If the marker had been instantiated
        from a markerPersist.to_marker it would have a
        _marker_persist attribute in order to retain
        the original created datetime.

        :param marker: :class: `furious.marker_tree.Marker`

        :return: :class: `MarkerPersist`
        """
        marker_persisted = None
        if hasattr(marker, '_marker_persist'):
            marker_persisted = marker._marker_persist
            marker_persisted.done = marker.done
            marker_persisted.result = marker.result
            marker_persisted.group = (
                ndb.Key('MarkerPersist', marker.group_id)
                if marker.group_id else None)
            marker_persisted.callbacks = marker.get_encoded_callbacks()
            marker_persisted.children = [
                ndb.Key('MarkerPersist', idx) for idx in
                marker.children_as_ids()
            ]

        if not marker_persisted:
            marker_persisted = cls(
                id=marker.id,
                group_id=marker.group_id,
                done=marker.done,
                result=marker.result,
                group=(ndb.Key('MarkerPersist', marker.group_id)
                       if marker.group_id else None),
                callbacks=marker.get_encoded_callbacks(),
                children=[ndb.Key('MarkerPersist', idx) for idx in
                          marker.children_as_ids()]
            )

        if marker.start_time:
            marker_persisted.created = marker.start_time

        return marker_persisted

    def to_marker(self):
        """Instantiate a Marker from a MarkerPersist.
        Setting the _marker_persist attribute to self
        in order to attach the original created
        datetime.

        :return: :class: `furious.marker_tree.Marker`
        """
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
            start_time=self.created,
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

    Results of an Async are saved to a separate MarkerResult
    entity, Allowing anyone to check the status without the
    overhead of the result data.

    :param marker: :class: `furious.marker_tree.Marker`.
    :param save_leaves: :class: `bool` whether to save
    this or children markers if they are leaves(no children).

    :return: :class: `MarkerPersist` and :class: `list` of
    :class: `ndb.Future` objects
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
    """ndb marker persist strategy
    this is called by a root marker.
    Persists all the markers to the datastore using the
    recursive _marker_persist and then waits for all
    the resulting futures to finish.

    :param marker: :class: `furious.marker_tree.Marker`.
    :param save_leaves: :class: `bool` whether to save
    this or children markers if they are leaves(no children).
    """
    mp, put_futures = _marker_persist(marker, save_leaves)
    Future.wait_all(put_futures)
    logging.debug("marker saved id: %s" % marker.id)
    return


def marker_get(idx, load_results=False):
    """Load a Marker from the datastore by it's id
    and optionally load the result data.

    :param idx: :class: `str` representing the id of an ndb.Key.
    :param load_results: :class: `bool` whether to load result data
    from the datastore.

    :return: :class: `furious.marker_tree.Marker`
    """
    key = ndb.Key('MarkerPersist', idx)

    if load_results:
        result_key = ndb.Key('MarkerResult', idx)
        # get both at the same time.
        marker_persisted, result = ndb.get_multi([key, result_key])
        if result:
            # Set the result data to the result attribute
            # of the marker_persisted.
            marker_persisted.result = result.result
    else:
        marker_persisted = key.get()

    if marker_persisted:
        return marker_persisted.to_marker()


def marker_get_multi(ids, load_results=False):
    """Asynchronously load markers.

    :param ids: :class: `list` representing the id of an ndb.Key.
    :param load_results: :class: `bool` whether to load result data
    from the datastore.

    :return: :class: `list` of :class: `furious.marker_tree.Marker`
    """
    # Create a list of MarkerPersist keys base on the ids.
    keys = [ndb.Key('MarkerPersist', idx) for idx in ids]

    # Request the MarkerPersists Asynchronously.
    marker_futures = ndb.get_multi_async(keys)

    # Request the MarkerResults Asynchronously.
    results_persisted = {}
    marker_result_futures = None
    if load_results:
        result_keys = [ndb.Key('MarkerResult', idx) for idx in ids]
        marker_result_futures = ndb.get_multi_async(result_keys)

    # With all MarkerResult's requested, iterate the futures
    # retrieving the result and build a dictionary keyed on id.
    if marker_result_futures:
        Future.wait_all(marker_result_futures)
        for future in marker_result_futures:
            result = future.get_result()
            if result:
                results_persisted[result.key.id()] = result

    # Wait for all the marker_futures to finish
    Future.wait_all(marker_futures)

    # Build a list of Markers and if there are results,
    # include them in the Marker.
    markers = []
    for future in marker_futures:
        marker_persisted = future.get_result()
        if marker_persisted:

            # If there is a result for this marker id, set it to
            # the marker_persisted result attribute.
            result = results_persisted.get(marker_persisted.key.id())
            if result:
                marker_persisted.result = result.result

            # Convert entity to a Marker and add to the markers list.
            markers.append(marker_persisted.to_marker())

    return markers


def marker_get_children(marker, load_results=False):
    """Loads all the children of a Marker.

    :param marker: :class: `furious.marker_tree.Marker`
    :param load_results: :class: `bool` whether to load result data
    from the datastore.

    :return: :class: `list` of :class: `furious.marker_tree.Marker`
    """
    children = marker_get_multi(
        marker.children_as_ids(), load_results=load_results)
    return children


def store_context(idx, context_dict):
    """Store a Context in the datastore in a ContextPersist
    entity.

    :param idx: :class: `str` used as the id of key of a ContextPersist.
    :param context_dict: `dict` representing a Context.
    """
    cp = ContextPersist(id=idx, data=context_dict)
    cp.put()


def load_context(idx):
    """Load a Context from a ContextPersist
    entity in the datastore.

    :param idx: :class: `str` the id of key of a ContextPersist.
    """
    cp = ContextPersist.get_by_id(idx)
    return cp.data
