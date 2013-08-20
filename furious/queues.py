#
# Copyright 2012 WebFilings, LLC
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

from random import shuffle
from threading import local


def select_queue(queue_group, queue_count=1):
    """Select an optimal queue to run a task in from the given queue group. By
    default, this simply randomly selects a queue from the group.

    TODO: leverage the taskqueue API to try and determine the best queue to
    use via a kwarg.
    """

    if not queue_group:
        return None

    if queue_count <= 0:
        raise Exception('Queue group must have at least 1 queue.')

    queue_lists = _get_from_cache('queues', default={})
    group_queues = queue_lists.setdefault(queue_group, [])

    if not group_queues:
        group_queues.extend('%s-%d' % (queue_group, i)
                            for i in xrange(queue_count))

        shuffle(group_queues)

    return group_queues.pop()


# Use a thread local cache to optimize performance and queue distribution.
_cache = local()


def _get_from_cache(key, default=None):
    """Fetch the value for the given key from the thread local cache. If the
    key does not exist, set it for the given default value and return the
    default.
    """

    if not key:
        return default

    if hasattr(_cache, key):
        return getattr(_cache, key)

    setattr(_cache, key, default)

    return default

