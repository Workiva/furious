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
import logging
import math
import uuid
import random
import string
logger = logging.getLogger('completion_marker')
# logger.setLevel(logging.DEBUG)

from furious.job_utils import decode_callbacks
from furious.job_utils import encode_callbacks
from furious.config import get_default_persistence_engine


persistence_module = get_default_persistence_engine()

BATCH_SIZE = 10
CHILDREN_ARE_LEAVES = True
CHILDREN_ARE_INTERNAL_VERTEXES = False


def random_alpha_numeric():
    """


    :return: a string of two random letters or digits
    """
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for x in range(2))


def ordered_random_ids(number_of_ids):
    """

    :param number_of_ids:
    :return: ordered list of random strings with order index as suffix
    """
    ids = set()
    while len(ids) < number_of_ids:
        ids.add(random_alpha_numeric())

    ids = list(ids)
    ids.sort()
    return [''.join([idx, str(i)]) for i, idx in enumerate(ids)]


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
    return int(n / BATCH_SIZE) * BATCH_SIZE


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
    return max((((round_down(n) + round_up(n % BATCH_SIZE)) / BATCH_SIZE) * 2 - 1) + n, 1)


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
    return max(((round_down(n) + round_up(n % BATCH_SIZE)) / BATCH_SIZE) * 2 - 1, 1)


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


class InvalidGroupId(Exception):
    """GroupId must be a basestring

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
    if not isinstance(group_id, basestring):
        raise InvalidGroupId("Not a valid group_id, expected "
                             "string, got {0}".format(group_id))
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
        raise InvalidLeafId("Id must be a basestring, "
                            "it is {0}, {0}".format(type(
                            persistence_id), persistence_id))


def first_iv_markers(markers):
    """
    Args:
        markers: a list of markers that start with a
        non-leaf marker
    Returns:
        the first contiguous list of non-leaf
        markers
    """
    first_ivs = []
    for marker in markers:
        if marker.is_leaf():
            break
        else:
            first_ivs.append(marker)
    return first_ivs


def group_into_internal_vertex_results(markers, leaf_combiner,
                                       grouped_results=None):
    """
    Args:
        markers: list of markers
        leaf_combiner: a function that will take a list of
            leaf markers and return one result
        grouped_results: a list of results of the combiner results
            and non-leaf results

        Returns:
            a list of results of the combiner results
            and non-leaf results
    """
    grouped_results = grouped_results or []
    leaf_markers = []
    for i, marker in enumerate(markers):
        if marker and not marker.is_leaf():
            if leaf_markers and leaf_combiner and \
                    callable(leaf_combiner):
                result = leaf_combiner(
                    [leaf_marker.result for leaf_marker
                     in leaf_markers if leaf_marker]
                )
                grouped_results.append(result)
            iv_markers = first_iv_markers(markers[i:])
            for iv in iv_markers:
                grouped_results.append(iv.result)
            return group_into_internal_vertex_results(
                markers[i + len(iv_markers):], leaf_combiner,
                grouped_results)
        elif marker:
            leaf_markers.append(marker)
    if leaf_markers and leaf_combiner and callable(leaf_combiner):
        result = leaf_combiner(
            [leaf_marker.result for leaf_marker
             in leaf_markers if leaf_marker]
        )
        grouped_results.append(result)
    return grouped_results


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
        self.done = options.get('done', False)
        self.result = options.get('result')
        self._update_done_in_progress = False
        self._work_time = options.get('work_time')
        self._options = options

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value
        self._options['id'] = value

    @property
    def work_time(self):
        return self._work_time

    @property
    def children(self):
        return self._children

    @children.setter
    def children(self, value):
        self._children = value

    def is_leaf(self):
        return not bool(self.children)

    def result_to_dict(self):
        return {
            'id': self.id,
            'job_time': self.work_time,
            'result': self.result
        }

    @classmethod
    def make_markers_for_tasks(cls, tasks, group_id=None,
                               context_callbacks=None):
        """

        :param tasks:
        :param group:
        :param context_callbacks:
        :return: :raise:
        """
        markers = []
        if group_id is None:
            # Bootstrap the top level context marker.
            logger.debug("bootstrapping group id")
            group_id = uuid.uuid4().hex

        if len(tasks) > BATCH_SIZE:
            # Make two internal vertex markers.
            # Recurse the first one with ten tasks
            # then recurse the second with the rest.
            first_tasks = tasks[:BATCH_SIZE]
            second_tasks = tasks[BATCH_SIZE:]

            first_group = Marker(
                id=uuid.uuid4().hex,
                group_id=group_id,
                callbacks=context_callbacks)
            first_group.children = cls.make_markers_for_tasks(
                tasks=first_tasks,
                group_id=first_group.id,
                context_callbacks=context_callbacks)

            second_group = Marker(
                id=uuid.uuid4().hex, group_id=group_id,
                callbacks=context_callbacks)
            second_group.children = cls.make_markers_for_tasks(
                second_tasks,
                group_id=second_group.id,
                context_callbacks=context_callbacks)

            # These two will be sibling nodes.
            markers.append(first_group)
            markers.append(second_group)
            return markers
        else:
            # Make leaf markers for the tasks.
            try:
                markers = []
                ids = ordered_random_ids(len(tasks))
                for index, task in enumerate(tasks):
                    idx = leaf_persistence_id_from_group_id(group_id,
                                                            ids[index])
                    # Assign this leaf marker id to the Async
                    # so when the task is processed, it
                    # can write this marker
                    task.id = idx
                    markers.append(Marker.from_async(task))

            except TypeError:
                raise
            return markers

    @classmethod
    def make_marker_tree_for_context(cls, context):
        root_marker = Marker(
            id=str(context.id),
            group_id=None,
            callbacks=context._options.get('callbacks'))

        # If no callbacks were given, check if this context
        # has a parent and if so, load parent and use
        # it's callbacks if it has any. If this context was
        # spawned by a leaf node it will use the
        # context callback of it's parent context.
        if not root_marker.callbacks:
            group_id = root_marker.get_group_id()
            if group_id:
                group_marker = Marker.get(group_id)
                if group_marker and group_marker.callbacks:
                    logger.debug("using parent callbacks for %s" %
                                 root_marker.id)
                    root_marker.callbacks = group_marker.callbacks

        if not context._tasks:
            # If a context is made without any tasks, it will
            # not complete, so this adds a do-nothing target.
            context.add(target=place_holder_target, args=[0])

        root_marker.children = Marker.make_markers_for_tasks(
            context._tasks,
            group_id=context.id,
            context_callbacks=context._options.get('callbacks')
        )

        return root_marker

    def children_to_dict(self):
        """
        The children property may contain IDs of children
        or Marker instances. Marker instances will be there
        when the graph is created and the IDs will be there
        when a marker is restored from the persistence
        layer with Marker.get(the_id).
        """
        return [child for child in self.children
                if isinstance(child, basestring)] \
            or \
               [child.to_dict() for child in self.children
                if isinstance(child, Marker)]

    def children_markers(self):
        ids = [child for child in self.children
               if isinstance(child, basestring)]

        child_markers = []
        if ids:
            child_markers = Marker.get_multi(ids)

        child_markers.extend([child for child in self.children
                              if isinstance(child, Marker)])
        return child_markers

    def children_as_ids(self):
        return [child for child in self.children
                if isinstance(child, basestring)] \
            or \
               [child.id for child in self.children
                if isinstance(child, Marker)]

    @classmethod
    def children_from_dict(cls, children_dict):
        """
        the list of children of a marker_dict may be
        IDs or they may be dicts representing child
        markers
        """
        return [child for child in children_dict
                if isinstance(child, basestring)] \
            or \
               [cls.from_dict(child_dict) for
                child_dict in children_dict
                if isinstance(child_dict, dict)]

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
        #        logger.debug("to dict %s"%self.id)
        options = copy.deepcopy(self._options)

        callbacks = self._options.get('callbacks')
        if callbacks:
            options['callbacks'] = encode_callbacks(callbacks)

        options['children'] = self.children_to_dict()
        options['work_time'] = self.work_time

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
            marker_dict.get('children', []))

        return cls(**marker_options)

    @classmethod
    def from_async(cls, async):
        if async is None:
            return
        group_id = None
        try:
            # Does the async have an id with a valid group id?
            group_id = leaf_persistence_id_to_group_id(async.id)
        except InvalidLeafId:
            pass
        callbacks = async._options.get('callbacks')
        marker = cls(id=async.id,
                     group_id=group_id,
                     callbacks=callbacks)
        return marker

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
        If a value is changed, it must be done in an idempotent way,
        such that if a value is changed because of a child, other
        simultaneous processes would make the same change
        """
        from furious.context import get_current_context
        from furious.context import NotInContextError

        save_leaves = True
        is_root_marker = False
        # Infer if this is the root marker of a graph
        # or else just a node saving it's state.
        for child in self.children:
            if isinstance(child, Marker):
                # Indicates this is the root of a graph
                # and when a graph is saved, don't
                # save the leaves.
                is_root_marker = True
                save_leaves = False

        if save_leaves and not self._update_done_in_progress:
            raise NotSafeToSave('must only save during update_done'
                                ' or if this is a root marker of a graph before the context'
                                ' has inserted tasks')

        if is_root_marker:
            try:
                current_context = get_current_context()
                if current_context and current_context.id == self.id and \
                        current_context._tasks_inserted:
                    raise NotSafeToSave('cannot save after tasks have'
                                        ' been inserted')
            except NotInContextError:
                pass

        if hasattr(persistence_module, 'marker_persist') and \
                callable(persistence_module.marker_persist):
            persistence_module.marker_persist(self, save_leaves)

    def _persist_whole_graph(self):
        """
        For those times when you absolutely want to
        save every marker in the tree. It will overwrite any
        existing Markers. Children of this marker which are
        only ids of markers will not be saved.
        """
        from furious.context import get_current_context
        from furious.context import NotInContextError

        save_leaves = True
        if not self.is_leaf():
            try:
                current_context = get_current_context()
                if current_context and current_context.id == self.id and \
                        current_context._tasks_inserted:
                    raise NotSafeToSave('cannot save after tasks have'
                                        ' been inserted')
            except NotInContextError:
                pass
        if hasattr(persistence_module, 'marker_persist') and \
                callable(persistence_module.marker_persist):
            persistence_module.marker_persist(self, save_leaves)

    @classmethod
    def get(cls, idx):
        if hasattr(persistence_module, 'marker_get') and \
                callable(persistence_module.marker_get):
            return persistence_module.marker_get(idx)

    @classmethod
    def get_multi(cls, ids):
        if hasattr(persistence_module, 'marker_get_multi') and \
                callable(persistence_module.marker_get_multi):
            return persistence_module.marker_get_multi(ids)

    def get_persisted_children(self):
        if hasattr(persistence_module, 'marker_get_children') and \
                callable(persistence_module.marker_get_children):
            return persistence_module.marker_get_children(self)

    def update_done(self, persist_first=False):
        """
        Args:
            persist_first: save any changes before bubbling
            up the tree. Used after a leaf task has been
            set as done.
        Returns:
            Boolean: True if done

        illustration of a way results are handled
        Marker tree
            o
            \
        o-----------o
        \           \
        --------   -----
        \ \ \ \ \  \ \ \ \
        o o o o o   o o o o
              \
              ---
              \ \ \
              o o o

        ================
        ints are the order of task results
            o
            \
        o-----------o
        \           \
        --------   -------
        \ \ \ \ \  \ \ \  \
        0 1 2 o 6  7 8 9  10
              \
              ---
              \ \ \
              3 4 5

        the leaf combiner would combine each contiguous
        group of leaf results
        and the internal vertex combiner will
        combine each group
        [[[0,1,2],[[3,4,5]],[6]],[[7,8,9,10]]]
        """
        count_update(self.id)
        self._update_done_in_progress = True

        # If a marker has just been changed
        # it must persist itself before checking if it's children
        # are all done and bubbling up. Doing so will allow it's
        # parent to know it's changed.
        logger.debug("update done for id: %s" % self.id)
        if persist_first:
            count_marked_as_done(self.id)
            self.persist()

        leaf = self.is_leaf()
        if leaf and self.done:
            logger.debug("leaf and done id: %s" % self.id)
            self.bubble_up_done()
            self._update_done_in_progress = False
            return True
        elif not leaf and not self.done:
            logger.debug("not leaf and not done yet id: %s" % self.id)
            children_markers = self.get_persisted_children()
            done_markers = [marker for marker in children_markers
                            if marker and marker.done]
            if len(done_markers) == len(self.children):
                self.done = True
                logger.debug("done now")
                if self.callbacks:
                    callbacks = decode_callbacks(self.callbacks)
                    leaf_combiner = callbacks.get('leaf_combiner')
                    internal_vertex_combiner = callbacks.get(
                        'internal_vertex_combiner')

                    internal_vertex_results = group_into_internal_vertex_results(
                        done_markers, leaf_combiner)

                    if internal_vertex_combiner:
                        result_of_combined_internal_vertexes = \
                            internal_vertex_combiner([result for result in
                                                      internal_vertex_results])

                        self.result = result_of_combined_internal_vertexes

                count_marked_as_done(self.id)
                self.persist()
                self._update_done_in_progress = False

                # Bubble up to tell the group marker to update done.
                self.bubble_up_done()
                return True

            self._update_done_in_progress = False
            return False
        elif self.done:
            logger.debug("already done id: %s" % self.id)
            self._update_done_in_progress = False

            # No need to bubble up, it would have been done already.
            return True

    def bubble_up_done(self):
        """
        If this marker has a group_id(the ID of it's parent node)
        load that parent marker and call update_done.
        If not, this is the root marker and call it's success
        callback
        """
        logger.debug("start bubble up")
        group_id = self.get_group_id()

        if group_id:
            parent_marker = Marker.get(group_id)
            if parent_marker:
                logger.debug("has parent bubble up")
                return parent_marker.update_done()
            logger.error("group marker %s did not load" % group_id)
        else:
            logger.debug("top level reached, job complete")
            success_callback = None
            if self.callbacks:
                callbacks = decode_callbacks(self.callbacks)
                success_callback = callbacks.get('success')

            if success_callback and callable(success_callback):
                success_callback(self.id, self.result)

            return True

    def _list_of_leaf_markers(self):
        """
        Recursively builds a list of all the leaf markers here or below
        this tree, sub-tree or leaf.
        It will retrieve child markers from the persistence layer
        if not already loaded
        """
        leaves = []
        if self.is_leaf():
            leaves.append(self)
        else:
            for child in self.children_markers():
                leaves.extend(child._list_of_leaf_markers())

        return leaves

    def delete_leaves(self):
        """
        TODO: delete all the sub leaf markers
        """
        pass

    def delete_children(self):
        """
        TODO: delete all sub markers
        """
        pass

    def count_nodes(self):
        count = 1
        for child in self.children:
            count += child.count_nodes()

        return count


def count_update(idx):
    """

    :param idx:
    :return:
    """
    return


def count_marked_as_done(idx):
    """

    :param idx:
    :return:
    """
    return


def place_holder_target(*args, **kwargs):
    """


    :return:
    """
    return


def handle_async_done(async):
    """
    Args:
        an Async instance

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
