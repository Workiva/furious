import logging
import uuid
from google.appengine.ext.ndb import Future
from furious.async import Async
from furious import context

__author__ = 'johnlockwood'
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
            done_markers = [marker for marker in children_markers if marker.done]
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
            children_markers = ndb.get_multi(self.children)
            done_markers = [marker for marker in children_markers if marker.done]
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

class Marker(object):
    def __init__(self,id=None,group_id=None,callback=None,children=[],
                 async=None):
        self.key = id
        self.group_id = group_id
        self.callback = callback
        self.children = children
        self.async = async

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self,key):
        self._key = key

    def to_dict(self):
        return {'key':self.key,
                'group_id':self.group_id,
                'callback':self.callback,
                'children':[child.to_dict() for child in self.children],
                'async':(self.async.to_dict() if self.async else None)}

    @classmethod
    def from_dict(cls,marker_dict):
        return cls(id=marker_dict['key'],
        group_id=marker_dict['group_id'],
        callback=marker_dict['callback'],
        children=[cls.from_dict(child_dict) for child_dict in marker_dict['children']],
        async=(marker_dict['async'].from_dict() if marker_dict['async'] else None))

    def _persist(self):
        mp = MarkerPersist(
            id=self.key,
            group_id=self.group_id,
            group = (ndb.Key('MarkerPersist',self.group_id) if self.group_id else None),
            callback=self.callback)
        put_futures = []
        for child in self.children:
            child_mp, child_futures = child._persist()
            put_futures.extend(child_futures)
            mp.children.append(child_mp.key)

        if self.async:
            mp.async = self.async.to_dict()
        put_future = mp.put_async()
        put_futures.append(put_future)
        return mp, put_futures

    def persist(self):
        mp, put_futures = self._persist()
        Future.wait_all(put_futures)
        return mp



def make_markers_for_asyncs(asyncs,group=None,context=None):
    markers = []
    if group is None:
#        bootstrap the top level context marker
        group_id = str(uuid.uuid4())
    else:
        group_id = group.key


    if len(asyncs) > 10:
        first_asyncs = asyncs[:10]#asyncs[:len(asyncs)/2]
        second_asyncs = asyncs[10:]#asyncs[len(asyncs)/2:]

        first_group = Marker(
                id=str(uuid.uuid4()),
                group_id=(group.key if group else ""),)
        first_group.children = make_markers_for_asyncs(first_asyncs,first_group)
        second_group = Marker(
                id=str(uuid.uuid4()),
                group_id=(group.key if group else ""),)
        second_group.children = make_markers_for_asyncs(second_asyncs,second_group)
        markers.append(first_group)
        markers.append(second_group)
        return markers
    else:
        try:
            markers = []
            for (index, async) in enumerate(asyncs):
                id = ",".join([str(group_id),str(index)])
                async._persistence_id = id
                markers.append(Marker(
                    id=id,
                    group_id=(group.key if group else ""),
                    async = async,
                    callback=""))

        except TypeError, e:
            raise
        return markers

def build_async_tree(asyncs,context=None):
    """
from furious.extras.appengine.ndb import make_markers_for_asyncs
from furious.extras.appengine.ndb import Marker
from furious.extras.appengine.ndb import build_async_tree
from furious.extras.appengine.ndb import print_marker_tree
from furious.async import Async
import uuid
import pprint

def whatup(num):
    return num*2

# Instantiate an Async object.
atasks = []
for index in range(1,87):
    atasks.append(Async(
        target=whatup, args=[100]))


marker = build_async_tree(atasks)
print_marker_tree(marker)
    """
    marker = Marker(id=str(uuid.uuid4()),group_id=None)

    marker.children = make_markers_for_asyncs(asyncs,group=marker)
    return marker

def print_marker_tree(marker):
    print_markers([marker],prefix="")

def print_markers(markers,prefix=""):
    for marker in markers:
        print prefix,marker.key, prefix, marker.group_id
        if isinstance(marker,Marker):
            print_markers(marker.children,prefix=prefix+"    ")
