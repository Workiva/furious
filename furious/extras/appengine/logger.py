#
# Copyright 2012 WebFilings, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import json
import logging
import os
import re
import time

from furious.config import get_config


def log(async, headers):
    task_info = _get_task_info(headers)

    log_task_info(task_info)

    if get_config().get('log_sockets', False):
        logging.info("Send async info over socket connection.")
        log_async(async, task_info)


def log_task_info(task_info):
    # TODO: Add async info to task_info dump.
    task_info = json.dumps(task_info)

    logging.debug('TASK-INFO: %s', json.dumps(task_info))


def log_async(async, task_info=None):
    import socket

    connection_address = get_config().get('log_socket_address')
    connection_port = int(get_config().get('log_socket_port', 1300))

    if not connection_address:
        logging.info("No connection address to log output.")
        return

    try:
        logging.info("Sending logs to %s:%s", connection_address,
                     connection_port)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((connection_address, connection_port))

        # TODO: Handle payload size.
        # TODO: Add app id
        payload = {
            'id': async.full_id,
            'url': async.function_path,
            'request_info': get_request_info(),
            'request_address': _get_env_info('APPLICATION_ID', 'NO_APPID'),
        }

        if task_info:
            payload.update(task_info)

        # Add url and request id as a key
        s.sendall(json.dumps(payload))
        s.close()
    except Exception, e:
        logging.info(e)


def get_request_info():
    return "%(app_id)s.%(version)s.%(module)s.%(instance_id)s#%(request)s" % {
        'app_id': _get_env_info('APPLICATION_ID', 'NO_APPID'),
        'version': _get_env_info('CURRENT_VERSION_ID', 'NO_VERSION'),
        'module': _get_env_info('CURRENT_MODULE_ID', 'NO_MODULE'),
        'instance_id': _get_env_info('INSTANCE_ID', 'NO_INSTANCE_ID'),
        'request': os.environ.get('REQUEST_LOG_ID', 'NO_REQUEST'),
    }


def _get_task_info(headers):
    """Processes the header from task requests to log analytical data."""
    ran_at = time.time()
    task_eta = float(headers.get('X-Appengine-Tasketa', 0.0))
    task_info = {
        'retry_count': headers.get('X-Appengine-Taskretrycount', ''),
        'execution_count': headers.get('X-Appengine-Taskexecutioncount', ''),
        'task_eta': task_eta,
        'ran': ran_at,
        'gae_latency_seconds': ran_at - task_eta
    }

    return task_info


def _get_env_info(env_id, default=''):
    """Return the string with special characters removed."""
    return _clean_string(os.environ.get(env_id, default))


def _clean_string(string):
    return re.sub('[^a-zA-Z0-9\n]', '_', string.strip())
