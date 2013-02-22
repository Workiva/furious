import uuid

from itertools import izip_longest
from itertools import ifilter

from furious.marker_tree import BATCH_SIZE

from furious.marker_tree.marker import Marker

GROUP_SIZE = 10


def grouper(n, iterable, fillvalue=None):
    """Collect data into fixed-length chunks or blocks
    from http://docs.python.org/2/library/itertools.html#recipes"""
    # grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


class WideTreeMarker(Marker):
    @classmethod
    def split_into_groups(cls, tasks, markers=None, group_id=None,
                          context_callbacks=None):
        markers = markers or []
        if len(tasks) > GROUP_SIZE * BATCH_SIZE:
            rest_grouped_tasks = tasks[GROUP_SIZE * BATCH_SIZE:]
            rest_grouped = cls(
                id=uuid.uuid4().hex,
                group_id=group_id,
                callbacks=context_callbacks)
            rest_grouped.children = cls.make_markers_for_tasks(
                tasks=rest_grouped_tasks,
                group_id=rest_grouped.id,
                context_callbacks=context_callbacks)

            markers.append(rest_grouped)

        this_grouped_tasks = tasks[:GROUP_SIZE * BATCH_SIZE]
        task_grouper = grouper(GROUP_SIZE, this_grouped_tasks)
        for group in task_grouper:
            this_group = cls(
                id=uuid.uuid4().hex,
                group_id=group_id,
                callbacks=context_callbacks)

            task_group = []
            for task in ifilter(lambda task: task is not None, group):
                task_group.append(task)

            this_group.children = cls.make_markers_for_tasks(
                tasks=task_group,
                group_id=this_group.id,
                context_callbacks=context_callbacks)
            markers.append(this_group)

        return markers
