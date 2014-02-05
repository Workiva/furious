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

from furious.config import get_config

QUEUE_COUNTS = 'queue_counts'


def select_queue(queue_group):
    """Select an optimal queue to run a task in from the given queue group. By
    default, this simply randomly selects a queue from the group.

    TODO: leverage the taskqueue API to try and determine the best queue to
    use via a kwarg.
    """

    if not queue_group:
        return None

    queue_counts = get_config().get(QUEUE_COUNTS)
    if not isinstance(queue_counts, dict):
        raise Exception('%s config property must be a dict.' % QUEUE_COUNTS)

    queue_count = queue_counts.get(queue_group, 0)

    if not isinstance(queue_count, int) or queue_count <= 0:
        raise Exception('Queue group must have at least 1 queue.')

    queue_lists = _get_from_cache('queues', default={})
    group_queues = queue_lists.setdefault(queue_group, [])

    if not group_queues:
        group_queues.extend('%s-%d' % (queue_group, i)
                            for i in xrange(queue_count))

        shuffle(group_queues)

    return group_queues.pop()


def _get_from_cache(key, default=None):
    """Fetch the value for the given key from the thread local context. If the
    key does not exist, set it for the given default value and return the
    default.
    """
    from furious.context._local import get_local_context

    if not key:
        return default

    local_context = get_local_context()

    if hasattr(local_context, key):
        return getattr(local_context, key)

    setattr(local_context, key, default)

    return default

