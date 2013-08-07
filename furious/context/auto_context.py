
from furious.context.context import Context


class AutoContext(Context):
    """Similar to context, but automatically inserts tasks asynchronously as
    they are added to the context.  Inserted in batches if specified.
    """

    def __init__(self, batch_size=None, **options):
        """Setup this context in addition to accepting a batch_size."""

        Context.__init__(self, **options)

        self.batch_size = batch_size

        self._num_tasks_inserted = 0

    def add(self, target, args=None, kwargs=None, **options):
        """Add an Async job to this context.

        The same as Context.add, but calls _auto_insert_check() to
        insert tasks automatically.
        """

        target = super(
            AutoContext, self).add(target, args, kwargs, **options)

        self._auto_insert_check()

        return target

    def _auto_insert_check(self):
        """Automatically insert tasks asynchronously.
        Depending on batch_size, insert or wait until next call.
        """

        if not self.batch_size:
            self._handle_tasks()
            return

        if len(self.tasks) >= self.batch_size:
            self._handle_tasks()

    def _handle_tasks(self):
        """Convert Async's into tasks, then insert them into queues.
        Similar to the default _handle_tasks, but don't mark all
        tasks inserted.
        """

        self._handle_tasks_insert(batch_size=self.batch_size)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """In addition to the default __exit__(), also mark all tasks
        inserted.
        """

        super(AutoContext, self).__exit__(exc_type, exc_val, exc_tb)

        # Mark all tasks inserted.
        self._tasks_inserted = True

        return False

