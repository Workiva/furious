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
from furious.job_utils import decode_callbacks
from furious.job_utils import encode_callbacks
from furious.job_utils import function_path_to_reference
from furious.job_utils import get_function_path_and_options
from furious.config import get_configured_persistence_module
persistence_module = get_configured_persistence_module()

BATCH_SIZE = 10
CHILDREN_ARE_LEAVES = True
CHILDREN_ARE_INTERNAL_VERTEXES = False

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

class InvalidLeafId(Exception):
    """
    This leaf id is invalid, it must be prefixed by a group id, comma
    separated
    """

class AsyncNeedsPersistenceID(Exception):
    """This Async needs to have a _persistence_id to create a Marker."""


def handle_done(async):
    return persistence_module.handle_done(async)

class Marker(object):
    def __init__(self, **options):
        """
        """
        self.key = options.get('id')
        assert self.key
        self.group_id = options.get('group_id')
        self.batch_id = options.get('batch_id')
        self.callbacks = options.get('callbacks')
        self.children = options.get('children') or []
        self.async = options.get('async')

        self.internal_vertex = options.get('internal_vertex')
        self.all_children_leaves = options.get('all_children_leaves')

        self._options = options

        self._persistence_engine = options.pop('persistence_engine', None)

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self,key):
        self._key = key

    def to_dict(self):
        import copy

        options = copy.deepcopy(self._options)

        if self._persistence_engine:
            options['persistence_engine'], _ = get_function_path_and_options(
                self._persistence_engine)

        callbacks = self._options.get('callbacks')
        if callbacks:
            options['callbacks'] = encode_callbacks(callbacks)

        options['children'] = [child.to_dict() for child in self.children]

        return options

    @classmethod
    def from_dict(cls, marker_dict):
        import copy

        marker_options = copy.deepcopy(marker_dict)

        persistence_engine = marker_options.pop('persistence_engine', None)
        if persistence_engine:
            marker_options['persistence_engine'] = function_path_to_reference(
                persistence_engine)

        # If there is are callbacks, reconstitute them.
        callbacks = marker_options.pop('callbacks', None)
        if callbacks:
            marker_options['callbacks'] = decode_callbacks(callbacks)

        marker_options['children'] = [
            cls.from_dict(child_dict) for
            child_dict in marker_dict['children']
        ]

        return cls(**marker_options)

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
            callbacks=async_dict.get('callbacks'),
            async=async_dict)

    @classmethod
    def from_async(cls, async):
        return cls.from_async_dict(async.to_dict())

    def persist(self):
        if self._persistence_engine and\
               self._persistence_engine.store_context_marker and\
               callable(self._persistence_engine.store_context_marker):
            return self._persistence_engine.store_context_marker(self)
        else:
            return persistence_module.persist(self)

    def put(self):
        """
        A Marker must only be saved during the update_done stage
        just after it has been found to be done because
        more then one process may be checking the done status.
        if a value is changed, it must be done in an idempotent way,
        such that if a value is changed because of a child, other
        simultaneous processes would make the same change
        """
        pass

    def handle_done(self):
        """
        TODO: move logic from ndb module to here,
        keeping persistence layer dumb
        """
        pass

    def bubble_up_done(self):
        """
        TODO: move logic from ndb module to here,
        keeping persistence layer dumb
        """
        pass

    def is_done(self):
        """
        TODO: move logic from ndb module to here,
        keeping persistence layer dumb
        """
        pass

    def _list_children_keys(self):
        """
        TODO: move logic from ndb module to here,
        keeping persistence layer dumb
        """
        pass

    def _list_of_leaf_keys(self):
        """
        TODO: move logic from ndb module to here,
        keeping persistence layer dumb
        """
        pass

    def delete_leaves(self):
        """
        TODO: move logic from ndb module to here,
        keeping persistence layer dumb
        """
        pass

    def delete_children(self):
        """
        TODO: move logic from ndb module to here,
        keeping persistence layer dumb
        """
        pass

    def count_nodes(self):
        count = 1
        for child in self.children:
            count += child.count_nodes()

        return count

def leaf_persistence_id_from_group_id(group_id,index):
    return ",".join([str(group_id), str(index)])

def leaf_persistence_id_to_group_id(persistence_id):
    try:
        parts = persistence_id.split(',')
        parts.pop()
        if not parts:
            raise InvalidLeafId("Id must be prefixed by a group id"
                "separated by a comma")
        group_id = ",".join(parts)
        return group_id
    except AttributeError:
        raise InvalidLeafId("Id must be a basestring")

def make_markers_for_tasks(tasks, group=None, batch_id=None,
                           context_callbacks=None):
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

        first_group = Marker(internal_vertex=True,
            id=uuid.uuid4().hex, group_id=group_id, batch_id=batch_id,
            callbacks=context_callbacks)
        first_group.children, first_group.all_children_leaves = \
                make_markers_for_tasks(first_tasks, first_group, batch_id,
                    context_callbacks)

        second_group = Marker(internal_vertex=True,
            id=uuid.uuid4().hex, group_id=group_id, batch_id=batch_id,
            callbacks=context_callbacks)
        second_group.children, second_group.all_children_leaves = \
                make_markers_for_tasks(second_tasks, second_group, batch_id,
                    context_callbacks)

        #these two will be the children of the caller
        markers.append(first_group)
        markers.append(second_group)
        return markers, CHILDREN_ARE_INTERNAL_VERTEXES
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
        return markers, CHILDREN_ARE_LEAVES
