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
from google.appengine.api import memcache
from google.appengine.ext.ndb import Future
from google.appengine.ext import ndb
from furious.job_utils import decode_callbacks

SIBLING_MARKERS_COMPLETE = True
SIBLING_MARKERS_INCOMPLETE = False

class Result(ndb.Model):
    result = ndb.JsonProperty()
    created = ndb.DateTimeProperty(auto_now_add=True)

class MarkerTree(ndb.Model):
    tree = ndb.JsonProperty()
    created = ndb.DateTimeProperty(auto_now_add=True)

class InternalVertexTree(ndb.Model):
    tree = ndb.JsonProperty()
    created = ndb.DateTimeProperty(auto_now_add=True)

class ContextPersist(ndb.Model):
    data = ndb.JsonProperty()
    created = ndb.DateTimeProperty(auto_now_add=True)


class MarkerPersist(ndb.Model):
    """
    """
    group_id = ndb.StringProperty(indexed=False)
    batch_id = ndb.StringProperty(indexed=False)
    group = ndb.KeyProperty(indexed=False)
    callbacks = ndb.JsonProperty()
    children = ndb.KeyProperty(repeated=True,indexed=False)
    done = ndb.BooleanProperty(default=False,indexed=False)
    siblings_done = ndb.BooleanProperty(default=False,indexed=False)
    async = ndb.JsonProperty()
    result = ndb.JsonProperty()
    all_children_leaves = ndb.BooleanProperty(indexed=False)
    internal_vertex = ndb.BooleanProperty(indexed=False)


    def is_done(self):
        if self.done:
            return True
        elif self.children:
            children_markers = ndb.get_multi(self.children)
            done_markers = [marker for marker in children_markers
                            if marker.done]
            if len(done_markers) == len(self.children):
                return True

    def bubble_up_done(self):

        if self.group:
            logging.info("bubble up")
            group_marker = self.group.get()
            if group_marker:
                return group_marker.update_done()
        else:
            #it is the top level
            logging.info("top level reached!")
            result = Result(
                id=self.key.id(),
                result=self.result)
            result.put()
            memcache.set("Furious:{0}".format(self.key.id()), "done")
            #context callback
            #cleanup
            self.delete_leaves()
            return True

    def _list_children_keys(self):
        """
        returns it's key along with all of it's children's keys
        """
        children_markers = ndb.get_multi(self.children)
        keys = []
        for child in children_markers:
            keys.extend(child._list_children_keys())

        keys.append(self.key)
        return keys

    def _list_of_leaf_keys(self):
        leaf_keys = []
        if self.children and self.all_children_leaves:
            leaf_keys = self.children
        elif self.children:
            children_markers = ndb.get_multi(self.children)
            for child in children_markers:
                leaf_keys.extend(child._list_of_leaf_keys())
        else:
            leaf_keys.append(self.key)

        return leaf_keys

    def delete_leaves(self):
        """
        this will delete all the leaf nodes of the graph
        the nodes that represent each task
        leaving only the structure needed to reach every
        node for the ability to reach a marker that may have
        executed more than once after the whole thing
        is done.
        """
        logging.info("delete leaves %s"%self.key)
        keys_to_delete = self._list_of_leaf_keys()
        ndb.delete_multi(keys_to_delete)

    def delete_children(self):
        logging.info("delete %s"%self.key)
        keys_to_delete = self._list_children_keys()
        ndb.delete_multi(keys_to_delete)

    def update_done(self):
        logging.info("update done")
        if not self.children and self.done:
            if self.bubble_up_done():
                pass
#                self.siblings_done = True
#                self.put()
            return True
        elif self.children and not self.done:
            #early false might be able to be detected here using a bitmap
            #though that may not really be too much of an optimization because
            # of
            #ndb's caching

            children_markers = ndb.get_multi(self.children)
            done_markers = [marker for marker in children_markers
                            if marker and marker.done]
            if len(done_markers) == len(self.children):
                self.done = True
                #simply set result to list of child results
                #this would be a custom aggregation function
                #context callback
                #flatten results
                result = []
                #if this is not an internal vertex
                #use the callbacks leaf_combiner function
                #to reduce the results
                #if it is an internal vertex
                #use the internal_vertex_combiner function
                #to  reduce the results
                combiner = None
                if self.callbacks:
                    callbacks = decode_callbacks(self.callbacks)
                    if self.internal_vertex and self.all_children_leaves:
                        logging.info('leaf_combiner')
                        combiner = callbacks.get('leaf_combiner')
                    elif self.internal_vertex:
                        logging.info('internal_vertex_combiner')
                        combiner = callbacks.get('internal_vertex_combiner')

                if combiner:
                    logging.info('combiner used')
                    self.result = combiner([marker.result for marker in
                                            done_markers if marker])
                else:
                    for marker in done_markers:
                        if isinstance(marker.result,list):
                            result.extend(marker.result)
                        else:
                            result.append(marker.result)
                    self.result = result
                self.put()
                #bubble up: tell group marker to update done
                if self.bubble_up_done():
                    #not sure if siblings_done is a useful flag
                    #because an internal vertex will not
                    #bubble up if it's already marked as none
                    self.siblings_done = True
                    self.put()

                return True
        elif self.done:
            # no need to bubble up, it would have been done already
            return True

    @classmethod
    def from_marker(cls,marker):
        marker_dict = marker.to_dict()
        return cls(
            id=marker.key,
            group_id=marker.group_id,
            batch_id=marker.batch_id,
            internal_vertex=marker.internal_vertex,
            all_children_leaves=marker.all_children_leaves,
            group = (ndb.Key('MarkerPersist',marker.group_id)
                     if marker.group_id else None),
            callbacks=marker_dict.get('callbacks'),
#            async = marker.async
        )


def _persist(marker):
    """
    ndb Marker persist strategy
    _persist is recursive, persisting all child markers
    asynchronously. It collects the put futures as it goes.
    persist waits for the put futures to finish.
    """

    mp = MarkerPersist.from_marker(marker)
    put_futures = []

    for child in marker.children:
        child_mp, child_futures = _persist(child)
        if child_mp:
            mp.children.append(child_mp.key)
        if child_futures:
            put_futures.extend(child_futures)

    if mp.children:
        #only add own marker there are children
        #it is a root or internal vertex marker
        put_future = mp.put_async()
        put_futures.append(put_future)
        logging.info("save because "
                     "it is an internal vertex %s"%marker.key)
    else:
        #don't persist leaf markers
        #they will be written when the task is processed
        logging.info("no initial save because "
                     "it is a leaf %s"%marker.key)

    return mp, put_futures

def persist(marker,save_tree=False):
    """
    ndb marker persist strategy
    this is called by a root marker
    """
#    import gaepdb;gaepdb.set_trace()
    mp, put_futures = _persist(marker)
    Future.wait_all(put_futures)
    logging.info("all root and internal vertex markers saved")
    if save_tree:
        defer_marker_tree_save(marker)
    return

def defer_marker_tree_save(marker):
    #save whole marker tree for diagnostics and possible error recovery
    from furious.async import Async

    # Instantiate an Async object.
    async_task = Async(
        target=save_marker_tree, args=[marker.to_dict()])

    # Insert the task to run the Async object, note that it may begin
    # executing immediately or with some delay.
    async_task.start()

def save_marker_tree(marker_tree):
    key = marker_tree.get('key')
    if key:
        markerTree = MarkerTree(
            id=marker_tree['key'],
            tree=marker_tree)
        tree_future = markerTree.put_async()
        tree_future.wait()

def handle_done(async):
    if async._persistence_id:
        logging.info("update mp: %s"%async._persistence_id)
        mp = MarkerPersist.get_by_id(async._persistence_id)
        if not mp:
            #create from async
            logging.info("MarkerPersist didn't exist, creating one from task")
            from furious.context.marker import Marker
            mp = MarkerPersist.from_marker(Marker.from_async(async))

        mp.done = True
        mp.result = async.result
        mp.put()
        mp.update_done()

class NDBContextPersistenceEngine(object):
    """
    This conforms to the persistence api
    storing just the context data
    """
    def store_context(self, id, context_dict):
        cp = ContextPersist(id=id, data=context_dict)
        cp.put()

    def load_context(self, id):
        cp = ContextPersist.get_by_id(id)
        return cp.data

    def store_context_marker(self,marker):
        persist(marker)
