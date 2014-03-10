
class CompletionEngine(object):

    def __init__(self, start_work=None, add_work=None, mark_work_complete=None):

        self._start = start_work
        self._add = add_work
        self._mark = mark_work_complete

    def start_work(self, work_ids, on_success, callback_kwargs,
                   on_failure=None, **kwargs):

        if not self._start:
            if not self._engine:
                self._prepare_default_engine()

            self._start = self._engine.start_work

        return self._start(work_ids, on_success, callback_kwargs, kwargs)

    def add_work(self, completion_id, work_ids, **kwargs):

        if not self._add:
            if not self._engine:
                self._prepare_default_engine()

            self._add = self._engine.add_work

        return self._add(completion_id, work_ids, **kwargs)

    def mark_work_complete(self, completion_id, work_id, **kwargs):

        if not self._mark:
            if not self._engine:
                self._prepare_default_engine()

        return self._mark(completion_id, work_id, kwargs)

    def _prepare_default_engine(self):

        if self._engine:
            return

        from furious.config import get_default_completion_engine

        self._engine = get_default_completion_engine()
