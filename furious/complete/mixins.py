from furious.job_utils import reference_to_path
from furious.job_utils import path_to_reference


class CompleteMixin(object):

    @property
    def completion_id(self):

        return self._options.get('completion_id', None)

    @completion_id.setter
    def completion_id(self, completion_id):

        self._options['completion_id'] = completion_id

    @property
    def on_success(self):

        callbacks = self._options.get('callbacks')

        if callbacks:
            return callbacks.get('on_success')

    @on_success.setter
    def on_success(self, on_success):
        callbacks = self._options.get('callbacks', {})

        if callable(on_success):
            on_success = reference_to_path(on_success)

        callbacks['on_success'] = on_success

        self._options['callbacks'] = callbacks

    @property
    def on_failure(self):
        callbacks = self._options.get('callbacks')

        if callbacks:
            return callbacks.get('on_failure')

    @on_failure.setter
    def on_failure(self, on_failure):

        callbacks = self._options.get('callbacks', {})

        if callable(on_failure):
            on_failure = reference_to_path(on_failure)

        callbacks['on_failure'] = on_failure

        self._options['callbacks'] = callbacks


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

    def add_work(self):

        return self._add

    def mark_work_complete(self):

        return self.mark

    def _prepare_default_engine(self):

        if self._engine:
            return

        from furious.config import get_default_completion_engine

        self._engine = get_default_completion_engine()
