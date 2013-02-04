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
