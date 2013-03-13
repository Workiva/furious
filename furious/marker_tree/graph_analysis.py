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
from furious.marker_tree import BATCH_SIZE


def count_update(idx):
    """
    :param idx: :class: `str`

    :return: :class: `str`
    """
    return idx


def count_marked_as_done(idx):
    """
    :param idx: :class: `str`

    :return: :class: `str`
    """
    return idx


def round_up(n):
    """n rounded up to the BATCH_SIZE.
    if BATCH_SIZE is 10: 6 => 10, 33 => 40

    :param n: :class: `int`

    :return: :class: `int`
    """
    return int(math.ceil(n / float(BATCH_SIZE))) * BATCH_SIZE


def round_down(n):
    """n rounded down to the BATCH_SIZE.
    if BATCH_SIZE is 10: 6 => 0, 33 => 30

    :param n: :class: `int`

    :return: :class: `int`
    """
    return int(n / BATCH_SIZE) * BATCH_SIZE


def tree_graph_growth(n):
    """
    :param n: :class: `int`

    :return: :class: `int`
        An integer representing the number
        of graph nodes will be used to split up n Asyncs

    For n == 16: This is what the graph structure would be
    ::
      o------------------------
            \                  \\
      ------o-----------   ----o-----
      \ \ \ \ \ \ \ \ \ \   \ \ \ \ \ \\
      o o o o o o o o o o    o o o o o o

    >>> [tree_graph_growth(n) for n in range(0,100,10)]
    [1, 11, 23, 35, 47, 59, 71, 83, 95, 107]
    """
    return max((((round_down(n) + round_up(n % BATCH_SIZE)) / BATCH_SIZE) * 2 - 1) + n, 1)


def initial_save_growth(n):
    """
    :param n: :class: `int`

    :return: :class: `int` An integer representing the number of
        graph nodes that will be
        saved to the persistence layer when the Context is started.
        There is no need to save the nodes of the Asyncs themselves,
        only the internal part of the graph.

    For n == 16: this is what the graph structure would be
    but we don't need to initially save the nodes of each Async(x)
    only the internal nodes(o). Each leaf node will be persisted
    later when the Async task completes because the id is attached
    to the task payload.
    ::
      o------------------------
            \                  \\
      ------o-----------   ----o-----
      \ \ \ \ \ \ \ \ \ \   \ \ \ \ \ \\
      x x x x x x x x x x    x x x x x x



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
