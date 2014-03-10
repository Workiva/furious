import logging

from furious.async import Async
from furious.context.context import Context
from furious.context import get_current_context
from furious.context import get_current_async

from furious.job_utils import decode_callbacks
from furious.job_utils import encode_callbacks
from furious.job_utils import get_function_path_and_options
from furious.job_utils import reference_to_path


def initialize_completion(node):
    """ Will create the initial completion graph with Kepler and then
    add any children required to it.

    :param node: :class: `Async` or `Context`
    """

    work_ids = [node.id]
    args = _gen_callback_args(node)

    completion_id = node.completion_engine.start_work(
        work_ids=work_ids,
        on_success=completion_callback,
        callback_kwargs=args,
        prefix="FURIOUS",
        entries_per_entity=20)

    node.completion_id = completion_id

    if isinstance(node, Async):
        node.process_results = _process_completion_result

    if isinstance(node, Context):
        add_work(node.completion_engine, completion_id, node,
                 node._tasks, args)

    return completion_id


def add_work(engine, completion_id, parent, asyncs, args=None):
    """Handles adding more work to the graph underneath an Async or a Context.

    :param completion_id: :class: `str`
    :param parent: :class: `Async` or `Context`
    :param asyncs: :class: `list` of `Async`
    :param args: : :class: `dict`

    """

    if not asyncs:
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
    engine.add_work(completion_id, work_ids, parent_id=parent.id,
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


def _process_completion_result(engine=None):
    """When the async is finished being run it will call a process results
    function that has been set in the options. This will do the work of
    marking the Async complete in the completion graph.

    """
    from furious.complete.engine import CompletionEngine
    from furious.processors import AsyncException

    if not engine:
        engine = CompletionEngine()

    async = get_current_async()

    if isinstance(async.result, AsyncException):
        payload = {'exception': async.result}
        engine.mark_work_complete(async.completion_id, async.id,
                                  fail=True, payload=payload)

    engine.mark_work_complete(async.completion_id, async.id)


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
