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

import random
import string


class InvalidLeafId(Exception):
    """
    This leaf id is invalid, it must be prefixed by a group id, comma
    separated
    """


class InvalidGroupId(Exception):
    """GroupId must be a basestring

    """


def random_alpha_numeric():
    """


    :return: a string of two random letters or digits
    """
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for x in range(2))


def ordered_random_ids(number_of_ids):
    """
    :param number_of_ids: :class: `int`
    :return: ordered list of random strings with order index as suffix
    """
    ids = set()
    while len(ids) < number_of_ids:
        ids.add(random_alpha_numeric())

    ids = list(ids)
    ids.sort()
    return [''.join([idx, str(i)]) for i, idx in enumerate(ids)]


def leaf_persistence_id_from_group_id(group_id, index):
    """
    :param group_id: :class: `str` the id of an internal
    node(internal vertex)
    :param index: :class: `str` uniquely identifies this
    node among it's sibling nodes.

    :return: :class: `str` a comma separated join of the two args
    """
    if not isinstance(group_id, basestring):
        raise InvalidGroupId("Not a valid group_id, expected "
                             "string, got {0}".format(group_id))
    return ",".join([str(group_id), str(index)])


def leaf_persistence_id_to_group_id(persistence_id):
    """
    :param persistence_id: :class: `str` of a node's id

    :return: :class: `str` representing the group id of a node.

    :raises: :class: `InvalidLeafId` if the id cannot be
    split by a comma or if the id is not a string.
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
