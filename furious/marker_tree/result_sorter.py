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


def first_iv_markers(markers):
    """
    :param markers: :class: `list` of :class: `Marker` starting with
    a non-leaf marker

    :return: :class: `list` of :class: `Marker` the first contiguous
    list of non-leaf markers
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
    :param markers: :class: `list` of :class: `Marker`
    :param leaf_combiner: :class: `function` taking a
    list of leaf markers and return one result.
    :param grouped_results: :class: `list` of results
    of the combiner results and non-leaf results

    :return: :class: `list` results of the combiner
    results and non-leaf results
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

