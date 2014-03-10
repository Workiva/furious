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
