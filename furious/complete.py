import logging

from kepler.api import mark_work_complete
from kepler.api import setup_completion_callback
from kepler.api import add_work_to_work_id

from .async import Async
from .context.context import Context
from .context import get_current_context
from .context import get_current_async

from .job_utils import decode_callbacks
from .job_utils import encode_callbacks
from .job_utils import get_function_path_and_options
from .job_utils import reference_to_path

from . import errors


def initialize_completion(node):
    """
    """

    work_ids = [node.id]
    args = _gen_callback_args(node)

    completion_id = setup_completion_callback(
        work_ids=work_ids,
        on_success=completion_callback,
        callback_args=args,
        prefix="FURIOUS",
        entries_per_entity=20)

    node.completion_id = completion_id

    if isinstance(node, Async):
        node.process_results = _process_completion_result

    if isinstance(node, Context):
        add_context_work(completion_id, node, node._tasks, args)

    return completion_id


def add_context_work(completion_id, parent, asyncs, args=None):

    if len(asyncs) < 1:
        return

    work_ids = []

    for async in asyncs:
        async.completion_id = completion_id
        async.process_results = _process_completion_result
        work_ids.append(async.id)

    if not args:
        args = _gen_callback_args(parent)

    add_work_to_work_id(completion_id, parent.id, work_ids,
                        on_success=completion_callback,
                        on_success_args=args)


def _gen_callback_args(node):

    callbacks = node._options.get('callbacks')
    args = {}
    if callbacks:
        args['callbacks'] = encode_callbacks(callbacks)

    return args


def handle_completion_start(node):

    callbacks = node._options.get('callbacks')

    if not callbacks:
        return

    if not node.on_success and not node.on_failure:
        return

    # If we are in a context with a completion id then we need to add to it
    current_context = None
    try:
        current_context = get_current_context()
    except errors.NotInContextError:
        logging.debug('no context')

    # not in a context or an async
    # no completion graph
    if not current_context or not current_context.completion_id:
        initialize_completion(node)
        return

    # we assume that we are in a context or part of an async
    if isinstance(node, Context):
        add_context_work(current_context.completion_id, node, node._tasks)
    else:
        add_context_work(
            current_context.completion_id, current_context, [node])


def _process_completion_result():
    from furious.processors import AsyncException

    async = get_current_async()

    if isinstance(async.result, AsyncException):
        mark_async_complete(async, True, {'exception': async.result})

    mark_async_complete(async)


def mark_async_complete(async, failed=False, failed_payload=None):

    mark_work_complete(async.completion_id, async.id, failed, failed_payload)


def execute_completion_callbacks(callbacks, failed=False, exception=None):

    if not callbacks:
        return

    callbacks = decode_callbacks(callbacks)
    callback = None

    if failed:
        callback = callbacks.get('on_failure')
        # TODO: Figure out how to merge the exception information into the
        # actual callback so that the failure function gets it.
        logging.info(exception)
    else:
        callback = callbacks.get('on_success')

    if not callback:
        return

    if isinstance(callback, (Context, Async)):
        callback.start()

    if callable(callback):
        function, options = get_function_path_and_options(callback)

        Async(target=function, options=options).start()


completion_callback = reference_to_path(execute_completion_callbacks)
