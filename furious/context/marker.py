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

import uuid

class MarkerMustBeRoot(Exception):
    """Marker must be a root marker, ie, have no group_id
    such as how it is created by a context._build_task_tree()
    """

class AsyncNeedsPersistenceID(Exception):
    """This Async needs to have a _persistence_id to create a Marker."""

class Marker(object):
    def __init__(self, id=None, group_id=None, batch_id=None,
                 callback=None, children=None, async=None):
        """
        """
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
                'async':self.async}

    @classmethod
    def from_dict(cls, marker_dict):
        return cls(id=marker_dict['key'],
            group_id=marker_dict['group_id'],
            batch_id=marker_dict['batch_id'],
            callback=marker_dict['callback'],
            children=[cls.from_dict(child_dict) for
                      child_dict in marker_dict['children']],
            async=marker_dict['async'])

    @classmethod
    def from_async_dict(cls, async_dict):
        persistence_id = async_dict.get('_persistence_id')
        if persistence_id is None:
            raise AsyncNeedsPersistenceID(
                'please assign a _persistence_id to the async'
                'before creating a marker'
            )
        group_id = leaf_persistence_id_to_group_id(persistence_id)
        return cls(id=persistence_id,
            group_id=group_id,
            batch_id=async_dict.get('_batch_id'),
            callback=async_dict.get('callback'),
            async=async_dict)

    @classmethod
    def from_async(cls, async):
        return cls.from_async_dict(async.to_dict())

    def persist(self):
        from furious.extras.appengine.ndb import persist
        if self.group_id:
            raise MarkerMustBeRoot(
                "You may only persist root markers")
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

