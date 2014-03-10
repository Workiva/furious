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

    @property
    def completion_engine(self):

        if self._engine:
            return self._engine

        from furious.complete.engine import CompletionEngine

        self._engine = CompletionEngine()
        return self._engine

    def _handle_completion_start(self):
        """ This will ensure that if there is any competion work to be done
        when an Async or a Context is 'started' that the work will be done
        before we insert any tasks.

        """
        from furious.context import get_current_context
        from furious.context import get_current_async
        from furious import errors
        from furious.complete.complete import initialize_completion
        from furious.complete.complete import add_work

        callbacks = self._options.get('callbacks')

        if not callbacks:
            return

        if not (self.on_success or self.on_failure):
            return

        # If we are in a Context or an Async with a completion id
        # then we need to add to that completion graph
        current_context = None
        try:
            current_context = get_current_context()

            if not current_context:
                current_context = get_current_async()

        except errors.NotInContextError:
            import logging
            logging.debug('no context')

        # not in a context or an async
        # need to build a new completion graph
        if not current_context or not current_context.completion_id:
            initialize_completion(self)
            return

        # in a context or part of an async
        # add to the existing completion graph
        if hasattr(self, '_tasks'):
            add_work(self.completion_engine, current_context.completion_id,
                     self, self._tasks)
        else:
            add_work(
                self.completion_engine, current_context.completion_id,
                current_context, [self])
