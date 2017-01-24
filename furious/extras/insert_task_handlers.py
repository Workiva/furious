from furious.context.context import _insert_tasks
from furious.context.context import _tasks_to_reinsert


def insert_tasks_ignore_duplicate_names(tasks, queue, *args, **kwargs):
    """Insert a batch of tasks into a specific queue.  If a
    DuplicateTaskNameError is raised, loop through the tasks and insert the
    remaining, ignoring and logging the duplicate tasks.

    Returns the number of successfully inserted tasks.
    """

    from google.appengine.api import taskqueue

    try:
        inserted = _insert_tasks(tasks, queue, *args, **kwargs)

        return inserted
    except taskqueue.DuplicateTaskNameError:
        # At least one task failed in our batch, attempt to re-insert the
        # remaining tasks.  Named tasks can never be transactional.
        reinsert = _tasks_to_reinsert(tasks, transactional=False)

        count = len(reinsert)
        inserted = len(tasks) - count

        # Our subsequent task inserts should raise TaskAlreadyExistsError at
        # least once, but that will be swallowed by _insert_tasks.
        for task in reinsert:
            inserted += _insert_tasks([task], queue, *args, **kwargs)

        return inserted
