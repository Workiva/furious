import uuid
from furious.extras.appengine.ndb import persist

class Marker(object):
    def __init__(self, id=None, group_id=None, callback=None, children=None,
                 async=None):
        self.key = id
        self.group_id = group_id
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
                'callback':self.callback,
                'children':[child.to_dict() for child in self.children],
                'async':(self.async.to_dict() if self.async else None)}

    @classmethod
    def from_dict(cls, marker_dict):
        return cls(id=marker_dict['key'],
            group_id=marker_dict['group_id'],
            callback=marker_dict['callback'],
            children=[cls.from_dict(child_dict) for
                      child_dict in marker_dict['children']],
            async=(marker_dict['async'].from_dict()
                   if marker_dict['async'] else None))



    def persist(self):
        return persist(self)


def make_markers_for_tasks(tasks, group=None):
    markers = []
    if group is None:
    #        bootstrap the top level context marker
        group_id = str(uuid.uuid4())
    else:
        group_id = group.key


    if len(tasks) > 10:
        first_tasks = tasks[:10]
        second_tasks = tasks[10:]

        first_group = Marker(
            id=str(uuid.uuid4()),
            group_id=(group.key if group else ""),)
        first_group.children = make_markers_for_tasks(
            first_tasks,first_group)
        second_group = Marker(
            id=str(uuid.uuid4()),
            group_id=(group.key if group else ""),)
        second_group.children = make_markers_for_tasks(
            second_tasks,second_group)
        markers.append(first_group)
        markers.append(second_group)
        return markers
    else:
        try:
            markers = []
            for (index, task) in enumerate(tasks):
                id = ",".join([str(group_id), str(index)])
                task._persistence_id = id
                markers.append(Marker(
                    id=id,
                    group_id=(group.key if group else ""),
                    async = task,
                    callback=""))

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
