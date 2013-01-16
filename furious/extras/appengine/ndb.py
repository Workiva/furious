import logging
from google.appengine.ext.ndb import Future
from google.appengine.ext import ndb

class Result(ndb.Model):
    result = ndb.JsonProperty()
    created = ndb.DateTimeProperty(auto_now_add=True)

class MarkerTree(ndb.Model):
    tree = ndb.JsonProperty()
    created = ndb.DateTimeProperty(auto_now_add=True)


class MarkerPersist(ndb.Model):
    """
    """
    group_id = ndb.StringProperty()
    group = ndb.KeyProperty()
    callback = ndb.StringProperty()
    children = ndb.KeyProperty(repeated=True)
    done = ndb.BooleanProperty(default=False)
    async = ndb.JsonProperty()
    result = ndb.JsonProperty()

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
                group_marker.update_done()
        else:
            #it is the top level
            logging.info("top level reached!")
            result = Result(
                id=self.key.id(),
                result=self.result)
            result.put()
            #context callback
            #cleanup
            self.delete_children()

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


    def delete_children(self):
        logging.info("delete %s"%self.key)
        keys_to_delete = self._list_children_keys()
        ndb.delete_multi(keys_to_delete)

    def update_done(self):
        logging.info("update done")
        if not self.children and self.done:
            self.bubble_up_done()
            return True
        elif self.children and not self.done:
            #early false might be able to be detected here using a bitmap
            #though that may not really be too much of an optimization because
            # of
            #ndb's caching

            children_markers = ndb.get_multi(self.children)
            done_markers = [marker for marker in children_markers
                            if marker.done]
            if len(done_markers) == len(self.children):
                self.done = True
                #simply set result to list of child results
                #this would be a custom aggregation function
                #context callback
                #flatten results
                result = []
                for marker in done_markers:
                    if isinstance(marker.result,list):
                        result.extend(marker.result)
                    else:
                        result.append(marker.result)
                self.result = result
                self.put()
                #bubble up: tell group marker to update done
                self.bubble_up_done()

                return True
        elif self.done:
            # no need to bubble up, it would have been done already
            return True


def _persist(marker):
    """
    ndb Marker persist strategy
    _persist is recursive, persisting all child markers
    asynchronously. It collects the put futures as it goes.
    persist waits for the put futures to finish.
    """
    mp = MarkerPersist(
        id=marker.key,
        group_id=marker.group_id,
        group = (ndb.Key('MarkerPersist',marker.group_id)
                  if marker.group_id else None),
        callback=marker.callback)
    put_futures = []
    for child in marker.children:
        child_mp, child_futures = _persist(child)
        put_futures.extend(child_futures)
        mp.children.append(child_mp.key)

    #does the async need to be stored?
    if marker.async:
        mp.async = marker.async.to_dict()
    put_future = mp.put_async()
    put_futures.append(put_future)
    return mp, put_futures

def persist(marker):
    """
    ndb marker persist strategy
    """
    mp, put_futures = _persist(marker)
    Future.wait_all(put_futures)

    #save whole marker tree for diagnostics and possible error recovery
    markerTree = MarkerTree(
        id=mp.key.id(),
        tree=marker.to_dict())
    tree_future = markerTree.put_async()
    return tree_future.wait


def handle_done(async):
    if async._persistence_id:
        logging.info("update mp: %s"%async._persistence_id)
        mp = MarkerPersist.get_by_id(async._persistence_id)
        if mp:
            mp.done = True
            mp.result = async.result
            mp.put()
            mp.update_done()
