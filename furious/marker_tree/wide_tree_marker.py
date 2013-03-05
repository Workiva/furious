import uuid

from itertools import ifilter
from itertools_recipes import grouper

from furious.marker_tree import BATCH_SIZE

from furious.marker_tree.marker import Marker

GROUP_SIZE = 10


class WideTreeMarker(Marker):
    @classmethod
    def split_into_groups(cls, tasks, markers=None,
                          group_id=None,
                          context_callbacks=None,
                          group_size=None,
                          batch_size=None):

        if batch_size and batch_size is not None:
            batch_size = int(batch_size)
        else:
            batch_size = BATCH_SIZE

        if group_size and group_size is not None:
            group_size = int(group_size)
        else:
            group_size = GROUP_SIZE

        markers = markers or []
        if len(tasks) > group_size * batch_size:
            rest_grouped_tasks = tasks[group_size * batch_size:]
            rest_grouped = cls(
                id=uuid.uuid4().hex,
                group_id=group_id,
                callbacks=context_callbacks)
            rest_grouped.children = cls.make_markers_for_tasks(
                tasks=rest_grouped_tasks,
                group_id=rest_grouped.id,
                context_callbacks=context_callbacks,
                group_size=group_size,
                batch_size=batch_size)

            markers.append(rest_grouped)

        this_grouped_tasks = tasks[:group_size * batch_size]
        task_grouper = grouper(batch_size, this_grouped_tasks)
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
                context_callbacks=context_callbacks,
                group_size=group_size,
                batch_size=batch_size)
            markers.append(this_group)

        return markers
