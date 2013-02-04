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
