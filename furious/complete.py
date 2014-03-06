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
    """ Will create the initial completion graph with Kepler and then
    add any children required to it.

    :param node: :class: `Async` or `Context`
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
    """Handles adding more work to the graph underneath an Async or a Context.

    :param completion_id: :class: `str`
    :param parent: :class: `Async` or `Context`
    :param asyncs: :class: `list` of `Async`
    :param args: : :class: `dict`

    """

    if len(asyncs) < 1:
        return

    work_ids = []
    if not args:
        args = _gen_callback_args(parent)

    for async in asyncs:
        async.completion_id = completion_id
        async.process_results = _process_completion_result
        work_ids.append(async.id)

    # The completion_callback will handle decoding the actual Async or Context
    # to run after the work is finished.
    add_work_to_work_id(completion_id, parent.id, work_ids,
                        on_success=completion_callback,
                        on_success_args=args)


def _gen_callback_args(node):
    """Helper to extract the callback dict from the options dict.
    """

    callbacks = node._options.get('callbacks')
    args = {}
    if callbacks:
        args['callbacks'] = encode_callbacks(callbacks)

    return args


def handle_completion_start(node):
    """ This will ensure that if there is any competion graph work to be done
    when an Async or a Context is 'started' that the work will be done before
    we insert any tasks.

    """
    callbacks = node._options.get('callbacks')

    if not callbacks:
        return

    if not node.on_success and not node.on_failure:
        return

    # If we are in a Context or an Async with a completion id
    # then we need to add to that completion graph
    current_context = None
    try:
        current_context = get_current_context()

        if not current_context:
            current_context = get_current_async()

    except errors.NotInContextError:
        logging.debug('no context')

    # not in a context or an async
    # need to build a new completion graph
    if not current_context or not current_context.completion_id:
        initialize_completion(node)
        return

    # in a context or part of an async
    # add to the existing completion graph
    if isinstance(node, Context):
        add_context_work(current_context.completion_id, node, node._tasks)
    else:
        add_context_work(
            current_context.completion_id, current_context, [node])


def _process_completion_result():
    """When the async is finished being run it will call a process results
    function that has been set in the options. This will do the work of
    marking the Async complete in the completion graph.

    """
    from furious.processors import AsyncException

    async = get_current_async()

    if isinstance(async.result, AsyncException):
        payload = {'exception': async.result}
        mark_work_complete(async.completion_id, async.id, True, payload)

    mark_work_complete(async.completion_id, async.id)


def execute_completion_callbacks(callbacks, failed=False, exception=None):
    """Decodes the on_success and on_failure callbacks which could be
    Asyncs or Contexts and then executes them.
    """

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
