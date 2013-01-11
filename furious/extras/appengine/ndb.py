import uuid

__author__ = 'johnlockwood'
from google.appengine.ext import ndb

class MarkerPersist(ndb.Model):
    """
    parent is Marker or no parent
    an async will be


    """
    group = ndb.StringProperty()
    callback = ndb.StringProperty()
    children = ndb.KeyProperty(repeated=True)
    done = ndb.BooleanProperty(default=False)
    async_id = ndb.StringProperty()
    result = ndb.JsonProperty()

class Marker(object):
    def __init__(self,id=None,group=None,callback=None,children=[],done=False,async_id=None,result=None):
        self.key = id
        self.group = group
        self.callback = callback
        self.children = children
        self.done = done
        self.async_id = async_id
        self.result = result

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self,key):
        self._key = key




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

def make_markers_for_asyncs(asyncs,group=None,callback=None):
    markers = []
    if group is None:
#        bootstrap the top level context marker
        group_id = str(uuid.uuid4())
    else:
        group_id = group.key


    if len(asyncs) > 10:
        first_asyncs = asyncs[:len(asyncs)/2]
        second_asyncs = asyncs[len(asyncs)/2:]
        id = ",".join([group_id,"0"])
        first_group = Marker(
                id=id,
                group=group,)
        first_group.key = id
        first_group.children = make_markers_for_asyncs(first_asyncs,first_group)
        id = ",".join([group_id,"1"])
        second_group = Marker(
                id=id,
                group=group,)
        second_group.key = id
        second_group.children = make_markers_for_asyncs(second_asyncs,second_group)
        markers.append(first_group)
        markers.append(second_group)
        return markers
    else:
        try:

            markers = [Marker(
                        id=",".join([str(group_id),str(index)]),
                        group=str(group_id),
                        async_id = str(async),
                        callback="") for (index, async) in enumerate(asyncs)]
        except TypeError, e:
            raise
        return markers

def print_markers(markers):
    for marker in markers:
        print marker.key
        if isinstance(marker,Marker):
            print_markers(marker.children)

class FuriousMassTracker(ndb.Model):
    data = ndb.JsonProperty()

    def build_data_with_asyncs(self,asyncs,level=0,parent_group_index=0):
        children = []

        gid = str(uuid.uuid4())
        gid = "%s:%s"%(level,parent_group_index)

        if len(asyncs) > 100000:
            first_asyncs = asyncs[:len(asyncs)/2]
            second_asyncs = asyncs[len(asyncs)/2:]
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