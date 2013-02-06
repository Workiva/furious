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
from furious.config import get_configured_persistence_module
persistence_module = get_configured_persistence_module()

BATCH_SIZE = 10
CHILDREN_ARE_LEAVES = True
CHILDREN_ARE_INTERNAL_VERTEXES = False
SIBLING_MARKERS_COMPLETE = True
SIBLING_MARKERS_INCOMPLETE = False

def round_up(n):
    """
    Args:
        n: an integer
    Returns:
        n rounded up to the BATCH_SIZE.
         if BATCH_SIZE is 10: 6 => 10, 33 => 40
    """
    return int(math.ceil(n / float(BATCH_SIZE))) * BATCH_SIZE


def round_down(n):
    """
    Args:
        n: an integer
    Returns:
        n rounded down to the BATCH_SIZE.
         if BATCH_SIZE is 10: 6 => 0, 33 => 30
    """
    return int(n/BATCH_SIZE)*BATCH_SIZE


def tree_graph_growth(n):
    """
    Args:
        n: an integer representing the number of Asyncs in a context job
    Returns:
        An integer representing the number of graph nodes will be used to
        split up n Asyncs

    for n == 16: this is what the graph structure would be
    o------------------------
          \                  \
    ------o-----------   ----o-----
    \ \ \ \ \ \ \ \ \ \  \ \ \ \ \ \
    o o o o o o o o o o  o o o o o o

    >>> [tree_graph_growth(n) for n in range(0,100,10)]
    [1, 11, 23, 35, 47, 59, 71, 83, 95, 107]
    """
    return max((((round_down(n)+round_up(n%BATCH_SIZE))/BATCH_SIZE)*2-1)+n,1)


def initial_save_growth(n):
    """
    Args:
        n: an integer representing the number of Asyncs in a context job
    Returns:
        An integer representing the number of graph nodes that will be
        saved to the persistence layer when the Context is started.
        There is no need to save the nodes of the Asyncs themselves,
        only the internal part of the graph.

    for n == 16: this is what the graph structure would be
    but we don't need to initially save the nodes of each Async(x)
    only the internal nodes(o). Each leaf node will be persisted
    later when the Async task completes because the id is attached
    to the task payload.
    o------------------------
          \                  \
    ------o-----------   ----o-----
    \ \ \ \ \ \ \ \ \ \  \ \ \ \ \ \
    x x x x x x x x x x  x x x x x x

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
    """
    counts the nodes of a marker graph
    """
    count = 1
    for child in marker.children:
        count += count_nodes(child)

    return count


class InvalidLeafId(Exception):
    """
    This leaf id is invalid, it must be prefixed by a group id, comma
    separated
    """

class NotSafeToSave(Exception):
    """
    A marker may only safely be saved during it's own
    update_done process or before the Async it represents
    has had it's task inserted.
    """

def leaf_persistence_id_from_group_id(group_id, index):
    """
    Args:
        group_id: the id of an internal node(internal vertex)
        index: a string that uniquely identifies this node among
        it's sibling nodes.
    Returns:
        A string that is a comma separated join of the two args
    """
    return ",".join([str(group_id), str(index)])


def leaf_persistence_id_to_group_id(persistence_id):
    """
    Args:
        persistence_id: a string of a node's id
    Returns:
        A string representing the group id of a node.
    Raises:
        InvalidLeafId if the id cannot be split by a comma
        or if the id is not a string
    """
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

class AsyncNeedsPersistenceID(Exception):
    """This Async needs to have a _persistence_id to create a Marker."""


class Marker(object):
    def __init__(self, **options):
        """
        """
        self._id = options.get('id')
        assert self.id
        self.group_id = options.get('group_id')
        self.callbacks = options.get('callbacks')
        self._children = options.get('children') or []
        self.async = options.get('async')

        self.internal_vertex = options.get('internal_vertex')
        self.all_children_leaves = options.get('all_children_leaves')

        self.done = options.get('done',False)
        self.result = options.get('result')
        self._update_done_in_progress = False

        self._options = options


    @property
    def id(self):
        return self._id


    @id.setter
    def id(self, value):
        self._id = value
        self._options['id'] = value


    @property
    def children(self):
        return self._children


    @children.setter
    def children(self,value):
        self._children = value


    def children_to_dict(self):
        """
        the children property may contain IDs of children
        or Marker instances.
        Marker instances will be there when the graph is
        created and the IDs will be there when a marker
        is restored from the persistence layer with Marker.get
        """
        return [child for child in self.children
                if isinstance(child,basestring)] \
                or\
               [child.to_dict() for child in self.children
                if isinstance(child,Marker)]


    def children_as_ids(self):
        return [child for child in self.children
                if isinstance(child,basestring)]\
                or\
               [child.id for child in self.children
                if isinstance(child,Marker)]


    @classmethod
    def children_from_dict(cls,children_dict):
        """
        the list of children of a marker_dict may be
        IDs or they may be dicts representing child
        markers
        """
        return [child for child in children_dict
                 if isinstance(child,basestring)]\
                 or\
                [cls.from_dict(child_dict) for
                 child_dict in children_dict
                 if isinstance(child_dict,dict)]


    def get_group_id(self):
        group_id = None
        try:
            #is the batch_id a valid leaf id?
            group_id = leaf_persistence_id_to_group_id(self.id)
        except InvalidLeafId:
            pass

        if self.group_id:
            #it is in internal vertex
            group_id = self.group_id

        return group_id


    def to_dict(self):
        import copy
#        logging.info("to dict %s"%self.id)
        options = copy.deepcopy(self._options)

        callbacks = self._options.get('callbacks')
        if callbacks:
            options['callbacks'] = encode_callbacks(callbacks)

        options['children'] = self.children_to_dict()

        return options


    @classmethod
    def from_dict(cls, marker_dict):
        import copy

        marker_options = copy.deepcopy(marker_dict)

        # If there is are callbacks, reconstitute them.
        callbacks = marker_options.pop('callbacks', None)
        if callbacks:
            marker_options['callbacks'] = decode_callbacks(callbacks)

        marker_options['children'] = cls.children_from_dict(
            marker_dict.get('children',[]))

        return cls(**marker_options)


    @classmethod
    def from_async_dict(cls, async_dict):
        id = async_dict.get('id')
        if id is None:
            raise AsyncNeedsPersistenceID(
                'please assign an id to the async '
                'before creating a marker'
            )
        group_id = leaf_persistence_id_to_group_id(id)
        return cls(id=id,
            group_id=group_id,
            callbacks=async_dict.get('callbacks'),
            async=async_dict)


    @classmethod
    def from_async(cls, async):
        return cls.from_async_dict(async.to_dict())


    @staticmethod
    def do_any_have_children(markers):
        """
        Args:
            markers: a list of Marker instances
        Returns:
            Boolean: True if any marker in the list
            has any ids in it's children property
        """
        for marker in markers:
            if marker.children:
                return True


    def persist(self):
        """
        Unless this is a root marker being saved before the Context
        it belongs to started, then
        a Marker must only be saved during the update_done stage
        just after it has been found to be done because
        more then one process may be checking the done status.
        if a value is changed, it must be done in an idempotent way,
        such that if a value is changed because of a child, other
        simultaneous processes would make the same change
        """
        from furious.context import get_current_context
        from furious.context import NotInContextError
        save_leaves = True
        is_root_marker = False
        #infer if this is the root marker of a graph
        #or else just a node saving it's state
        for child in self.children:
            if isinstance(child, Marker):
                #indicates this is the root of a graph
                #and when a graph is saved, don't
                #save the leaves
                is_root_marker = True
                save_leaves = False

        if save_leaves and not self._update_done_in_progress:
            raise NotSafeToSave('must only save during update_done'
            ' or if this is a root marker of a graph before the context'
            ' has inserted tasks')

        if is_root_marker:
            current_context = None
            try:
                current_context = get_current_context()
                if current_context and current_context.id == self.id and\
                   current_context._tasks_inserted:
                    raise NotSafeToSave('cannot save after tasks have'
                                    ' been inserted')
            except NotInContextError:
                pass

        if hasattr(persistence_module,'marker_persist') and\
                callable(persistence_module.marker_persist):
            persistence_module.marker_persist(self, save_leaves)


    @classmethod
    def get(cls,id):
        if hasattr(persistence_module,'marker_get') and\
           callable(persistence_module.marker_get):
            return persistence_module.marker_get(id)


    def get_persisted_children(self):
        if hasattr(persistence_module,'marker_get_children') and\
                callable(persistence_module.marker_get_children):
            return persistence_module.marker_get_children(self)


    def update_done(self,persist_first=False):
        count_update(self.id)
        self._update_done_in_progress = True
        # if a marker has just been changed
        # it must persist itself before checking if it's children
        # are all done and bubbling up. Doing so will allow it's
        # parent to know it's changed
        if persist_first:
            self.persist()
        if not self.children and self.done:
            if self.bubble_up_done():
                pass
            return True
        elif self.children and not self.done:
            children_markers = self.get_persisted_children()
            done_markers = [marker for marker in children_markers
                            if marker and marker.done]
            if len(done_markers) == len(self.children):
                self.done = True
                result = []
                combiner_func = None
                if self.all_children_leaves:
                    #a leaf child may have been replaced with a Context
                    #which makes all_children_leaves False
                    self.all_children_leaves = (not Marker.
                        do_any_have_children(children_markers))
        self._update_done_in_progress = False


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


def count_update(id):
    return


def handle_async_done(async):
    """
    This will mark and async as done and will
    begin the process to see if all the other asyncs
    in it's context, if it has one, are done
    """
    if async.id:
        marker = Marker.get(async.id)
        if not marker:
            marker = Marker.from_async(async)
        marker.done = True
        marker.result = async.result
        marker.update_done(persist_first=True)
