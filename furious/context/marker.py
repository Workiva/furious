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

import math
import uuid

BATCH_SIZE = 10

def round_up(n):
    return int(math.ceil(n / float(BATCH_SIZE))) * BATCH_SIZE

def round_down(n):
    return int(n/BATCH_SIZE)*BATCH_SIZE

def tree_graph_growth(n):
    """
    function for the size of the tree graph based on the
    number of tasks
    >>> [tree_graph_growth(n) for n in range(0,100,10)]
    [1, 11, 23, 35, 47, 59, 71, 83, 95, 107]
    """
    return max((((round_down(n)+round_up(n%BATCH_SIZE))/BATCH_SIZE)*2-1)+n,1)

def initial_save_growth(n):
    """
    This is the growth function of the number internal vertexes and root.
    It splits up the tasks into groups of 10 or less
    linking them together in a tree graph with the context at the root.
    as long as the root and internal vertexes are saved before the batch
    starts executing, as the tasks are processed, they can save their
    marker as a done and bubble up the done event which loads
    the marker of their group_id and runs it's own update_done
    process
    Before(save initial markers for all the vertexes):
    >>> [tree_graph_growth(n) for n in range(0,100,10)]
    [1, 11, 23, 35, 47, 59, 71, 83, 95, 107]

    After(save only root and internal vertexes):
    >>> [initial_save_growth(n) for n in range(0,100,10)]
    [1, 1, 3, 5, 7, 9, 11, 13, 15, 17]
    """
    return max(((round_down(n)+round_up(n%BATCH_SIZE))/BATCH_SIZE)*2-1,1)

def count_nodes(marker):
    count = 1
    for child in marker.children:
        count += count_nodes(child)

    return count

class MarkerMustBeRoot(Exception):
    """Marker must be a root marker, ie, have no group_id
    such as how it is created by a context._build_task_tree()
    """

class MustNotBeInternalVertex(Exception):
    """Marker must be a root marker or be a leaf marker
    to persist. leaf, because more asyncs could be added
    to an existing one
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
        return persist(self)

    def count_nodes(self):
        count = 1
        for child in self.children:
            count += child.count_nodes()

        return count

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


    if len(tasks) > BATCH_SIZE:
        #make two internal vertex markers
        #recurse the first one with ten tasks
        #and recurse the second with the rest
        first_tasks = tasks[:BATCH_SIZE]
        second_tasks = tasks[BATCH_SIZE:]

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
