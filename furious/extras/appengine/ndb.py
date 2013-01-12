import uuid
from furious.async import Async
from furious import context

__author__ = 'johnlockwood'
from google.appengine.ext import ndb

class MarkerPersist(ndb.Model):
    """
    parent is Marker or no parent
    an async will be

    """
    group_id = ndb.StringProperty()
    callback = ndb.StringProperty()
    children = ndb.KeyProperty(repeated=True)
    done = ndb.BooleanProperty(default=False)
    async = ndb.JsonProperty()
    result = ndb.JsonProperty()


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

    def persist(self):
        mp = MarkerPersist(
            id=self.key,
            group_id=self.group_id,
            callback=self.callback)
        mp.children = [child.persist().key for child in self.children ]
        if self.async:
            mp.async = self.async.to_dict()
        mp.put()
        return mp




def count_group_done(marker):
    group = marker.key.parent()
    if group:
        group_marker = ndb.get(group)
        done_children_markers = len([marker for marker \
                                     in ndb.get_multi(group.children) \
                                     if marker.done])
        if len(group.children) == done_children_markers:
            return count_group_done(group_marker.key)

    else:
        #call group callback
        from furious.async import Async

        # Instantiate an Async object.
        async_task = Async(
            target=marker.callback)

        # Insert the task to run the Async object, note that it may begin
        # executing immediately or with some delay.
        async_task.start()
        return

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

#            markers = [Marker(
#                        id=",".join([str(group_id),str(index)]),
#                        group_id=(group.key if group else ""),
#                        async = async,
#                        callback="") for (index, async) in enumerate(asyncs)]
#
#            for index, marker in enumerate(markers):
#                marker.async._persistence_id =",".join([str(group_id),str(index)])

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
from furious.extras.appengine.ndb import FuriousMassTracker
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

class FuriousMassTracker(ndb.Model):
    data = ndb.JsonProperty()

    def build_data_with_asyncs(self,asyncs,level=0,parent_group_index=0):
        children = []

        gid = str(uuid.uuid4())
        gid = "%s:%s"%(level,parent_group_index)

        if len(asyncs) > 100000:
            first_asyncs = asyncs[:10]#asyncs[:len(asyncs)/2]
            second_asyncs = asyncs[10:]#asyncs[len(asyncs)/2:]
            first_group_children = self.build_data_with_asyncs(first_asyncs,level+1,0)
            second_group_children = self.build_data_with_asyncs(second_asyncs,level+1,1)
            children.append(first_group_children)
            children.append(second_group_children)

        else:
            children = []
            for index,async in enumerate(asyncs):
                id = index
                async.uuid = id
                async.gid = gid
#                children.append({'id':id,'pid':gid,'done':0,'async':async.to_dict()})
                children.append({'id':id,'pid':gid,'done':0})

#            children = [{'id':uuid.uuid4(),'async':async.to_dict()} for async in asyncs]

        return {'gid':gid,'done':0,'children':children}

    @classmethod
    def with_asyncs(cls,asyncs):
        fmt = cls()
        fmt.data = fmt.build_data_with_asyncs(asyncs)
        return fmt

    def mark_async_done(self,async,gid,id):
        pass