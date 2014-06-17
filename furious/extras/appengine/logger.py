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

DEFAULT_SOCKET_PORT = 1300


class CONNECTIONS:

    HTTP = 'http'
    TCP = 'tcp'
    UDP = 'upd'


def log(async, headers):
    """Log the gae request info out to the default python logger. If an
    external logger is configured trigger the external logging of the same info
    and the specific async info.
    """
    task_info = _get_task_info(headers)

    logging.debug('TASK-INFO: %s', json.dumps(task_info))

    config = get_config()

    if config.get('log_external', False):
        logging.debug("Send async info over external connection.")
        log_async_info_external(config, async, task_info)


def log_async_info_external(config, async, task_info=None):
    """Log the task info and async information (id, path info, url, etc) to an
    external logger. It will serialize a json payload to pass to an http, udp,
    or tcp connection. Add settings are pulled from the config file. If any
    errors are hit this will fail silently as we don't want this to cause the
    request to error out. Also sets a 2 second dealine on the request which can
    be overridden in the config settings.
    """

    payload = _make_payload(async, task_info)

    if not payload:
        logging.debug("Unable to get task payload.")
        return

    connection_address = config.get('log_external_address')

    if not connection_address:
        logging.debug("No connection address to log output.")
        return

    connection_protocol = config.get('log_external_protocol', CONNECTIONS.HTTP)

    connection_port = config.get('log_external_port')
    connection_deadline = int(config.get('log_external_deadline', 2))

    if connection_protocol.lower() == CONNECTIONS.HTTP:
        _log_async_info_to_http(connection_address, connection_port,
                                connection_deadline, payload)
    else:
        _log_async_info_to_socket(connection_protocol, connection_address,
                                  connection_port, connection_deadline,
                                  payload)


def _log_async_info_to_http(connection_address, connection_port,
                            connection_deadline, payload):
    """Return a flag for a successful http request to send the payload to the
    passed in address and port.
    """
    from google.appengine.api import urlfetch

    try:
        url = _build_url(connection_address, connection_port)

        logging.debug("Sending Async Info %s to %s", payload, url)

        result = urlfetch.fetch(url, method=urlfetch.POST, payload=payload,
                                deadline=connection_deadline)

        if result.status_code != 200:
            logging.debug("Unable to log async info to %s reason: %s.", url,
                          result.content if result.content else '')
            return False

        return True

    except Exception, e:
        logging.debug(e)
        return False


def _log_async_info_to_socket(connection_protocol, connection_address,
                              connection_port, connection_deadline, payload):
    """Return a flag for a successful tcp or udp request to send the payload
    to the passed in address and port.
    """
    import socket

    if not connection_port:
        connection_port = DEFAULT_SOCKET_PORT

    if not isinstance(connection_port, int):
        connection_port = int(connection_port)

    try:
        logging.debug("Sending logs to %s:%s", connection_address,
                      connection_port)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(connection_deadline)
        s.connect((connection_address, connection_port))

        # Add url and request id as a key
        s.sendall(payload)
        s.close()

        return True

    except Exception, e:
        logging.debug(e)
        return False


def _build_url(connection_address, connection_port):
    """Return a properly put together url string for the passed in address and
    port. If the address already has a port set and there's a port passed in
    the one on the address will be kept and the passed in one ignored.
    """

    if not connection_address.startswith("http"):
        connection_address = "http://" + connection_address

    if connection_address.endswith("/"):
        connection_address = connection_address.rstrip("/")

    add_split = connection_address.split(':')

    if connection_port and len(add_split) < 3:
        connection_address = "%s:%s" % (connection_address,
                                        str(connection_port))

    return connection_address


def _make_payload(async, task_info=None):
    """Create a dict consisting of the async full id, async function path as
    the url, request info, application id as the address and combine it with
    the passed in task info. Then json dumps that and return it.
    """

    if not async:
        return

    try:
        # TODO: Handle payload size.
        # TODO: Add app id
        payload = {
            'id': async.full_id,
            'url': async.function_path,
            'request_info': _get_request_info(),
            'request_address': _get_env_info('APPLICATION_ID', 'NO_APPID'),
        }

        if task_info:
            payload.update(task_info)

        payload = json.dumps(payload)
    except Exception, e:
        logging.debug(e)
        return None

    return payload


def _get_request_info():
    """Return a string concatenation of GAE request info pulled from os.environ
    and cleaned of special characters. All task info not able to be set will be
    an empty string.

    String format: app_id.app_version.module_name.instance_id#request_id
    """

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
    """Return the string pulled from os.environ with special characters
    removed.
    """
    return _clean_string(os.environ.get(env_id, default))


def _clean_string(string):
    """Return the string with special characters removed."""
    return re.sub('[^a-zA-Z0-9\n]', '_', string.strip())
