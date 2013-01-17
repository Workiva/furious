import uuid
from furious.extras.appengine.ndb import persist

class AsyncNeedsPersistenceID(Exception):
    """This Async needs to have a _persistence_id to create a Marker."""

class Marker(object):
    def __init__(self, id=None, group_id=None, batch_id=None,
                 callback=None, children=None, async=None):
        self.key = id
        self.group_id = group_id
        self.batch_id = batch_id
        self.callback = callback
        self.children = children or []
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
                'batch_id':self.batch_id,
                'callback':self.callback,
                'children':[child.to_dict() for child in self.children],
                'async':(self.async.to_dict() if self.async else None)}

    @classmethod
    def from_dict(cls, marker_dict):
        return cls(id=marker_dict['key'],
            group_id=marker_dict['group_id'],
            batch_id=marker_dict['batch_id'],
            callback=marker_dict['callback'],
            children=[cls.from_dict(child_dict) for
                      child_dict in marker_dict['children']],
            async=(marker_dict['async'].from_dict()
                   if marker_dict['async'] else None))

    @classmethod
    def from_async_dict(cls, async_dict):
        persistence_id = async_dict.get('_persistence_id')
        if persistence_id is None:
            raise AsyncNeedsPersistenceID(
                'please assign a _persistence_id to the async'
                'before creating a marker'
            )
        id_parts = persistence_id.split(',')
        group_id = id_parts[0]
        return cls(id=persistence_id,
            group_id=group_id,
            batch_id=async_dict.get('_batch_id'),
            callback=async_dict.get('callback'),
            async=async_dict)

    @classmethod
    def from_async(cls, async):
        return cls.from_async_dict(async.to_dict())
#        persistence_id = async._persistence_id
#        if persistence_id is None:
#            raise AsyncNeedsPersistenceID(
#                'please assign a _persistence_id to the async'
#                'before creating a marker'
#            )
#        id_parts = persistence_id.split(',')
#        group_id = id_parts[0]
#        return cls(id=persistence_id,
#            group_id=group_id,
#            batch_id=async._batch_id,
#            callback=async.callback,
#            async=async.to_dict())



    def persist(self):
        return persist(self)

def leaf_persistence_id_from_group_id(group_id,index):
    return ",".join([str(group_id), str(index)])

def leaf_persistence_id_to_group_id(persistence_id):
    return persistence_id.split(',')[0]

def make_markers_for_tasks(tasks, group=None, batch_id=None):
    """
    batch_id is the id of the root
    """
    markers = []
    if group is None:
    #        bootstrap the top level context marker
        group_id = uuid.uuid4().hex
    else:
        group_id = group.key


    if len(tasks) > 10:
        #make two internal vertex markers
        #recurse the first one with ten tasks
        #and recurse the second with the rest
        first_tasks = tasks[:10]
        second_tasks = tasks[10:]

        first_group = Marker(
            id=uuid.uuid4().hex, group_id=group_id, batch_id=batch_id,)
        first_group.children = make_markers_for_tasks(
            first_tasks, first_group, batch_id)

        second_group = Marker(
            id=uuid.uuid4().hex, group_id=group_id, batch_id=batch_id)
        second_group.children = make_markers_for_tasks(
            second_tasks, second_group, batch_id)

        #these two will be the children of the caller
        markers.append(first_group)
        markers.append(second_group)
        return markers
    else:
        #make leaf markers for the tasks
        try:
            markers = []
            for (index, task) in enumerate(tasks):
                id = leaf_persistence_id_from_group_id(group_id,index)
                task._persistence_id = id
                task._batch_id = batch_id
                markers.append(Marker.from_async(task))

        except TypeError, e:
            raise
        return markers

def print_marker_tree(marker):
    print_markers([marker], prefix="")

def print_markers(markers, prefix=""):
    for marker in markers:
        print prefix,marker.key, prefix, marker.group_id
        if isinstance(marker, Marker):
            print_markers(marker.children,prefix=prefix+"    ")
