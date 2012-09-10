
try:
    import json
except ImportError:
    import simplejson as json

import logging

from ..async import Async
from ..async import run_job


def process_async_task(headers, request_body):
    """Process an Async task and execute the requested function."""
    async_options = json.loads(request_body)
    work = Async.from_dict(async_options)

    logging.info(work._function_path)

    run_job(work)

    return 200, work._function_path



