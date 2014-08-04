#
# Copyright 2013 WebFilings, LLC
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
import logging

from furious.config import get_config

from furious.extras.stat_recorder import Recorder

DEFAULT_SOCKET_PORT = 1300


class CONNECTIONS:

    HTTP = 'http'
    TCP = 'tcp'
    UDP = 'upd'


def furious_wsgi_middleware(app):

    def furious_wsgi_wrapper(environ, start_response):
        """Outer wrapper function around the WSGI protocol.

        The top-level furious() function returns this
        function to the caller instead of the app class or function passed
        in.  When the caller calls this function (which may happen
        multiple times, to handle multiple requests) this function
        instantiates the app class (or calls the app function), sandwiched
        between calls to start_recording() and end_recording() which
        manipulate the recording state.

        The signature is determined by the WSGI protocol.
        """
        recorder = Recorder(environ)

        try:
            result = app(environ, start_response)
        except Exception:
            recorder.status_code = 500
            log_stats(recorder)
            raise

        if result is not None:
            for value in result:
                yield value

        recorder.status_code = 200
        log_stats(recorder)

        if hasattr(result, 'close'):
            result.close()

    return furious_wsgi_wrapper


def log_stats(recorder):
    """Log the request and async stats to the listener."""
    payload = recorder.get_payload()

    if payload:
        log_async_info_external(payload)


class StatConnection(object):

    def __init__(self, config=None):
        self._config = config or get_config()

        self._connection_config = (
            self._config.get('stat_logger_connection', {})
            if self._config else {})

        self._url = None

    @property
    def address(self):
        return self._connection_config.get('address', 'localhost')

    @property
    def protocol(self):
        return self._connection_config.get('protocol', CONNECTIONS.HTTP)

    @property
    def port(self):
        return int(self._connection_config.get('port', 80))

    @property
    def deadline(self):
        return int(self._connection_config.get('deadline', 2))


def log_async_info_external(payload):
    """Log the task info and async information (id, path info, url, etc) to an
    external logger. It will serialize a json payload to pass to an http, udp,
    or tcp connection. Add settings are pulled from the config file. If any
    errors are hit this will fail silently as we don't want this to cause the
    request to error out. Also sets a 2 second dealine on the request which can
    be overridden in the config settings.
    """
    connection = StatConnection()

    if connection.protocol == CONNECTIONS.HTTP:
        _log_async_info_to_http(connection, payload)
    else:
        _log_async_info_to_socket(connection, payload)


def _log_async_info_to_http(connection, payload):
    """Return a flag for a successful http request to send the payload to the
    passed in address and port.
    """
    try:
        import urllib2

        url = connection.address

        if not (url and payload):
            return False

        logging.info("Sending Async Info %s to %s", payload, url)

        urllib2.urlopen(url, data=payload, timeout=connection.deadline)

        return True
    except Exception, e:
        logging.info(e)
        return False


def _log_async_info_to_socket(connection, payload):
    """Return a flag for a successful tcp or udp request to send the payload
    to the passed in address and port.
    """
    import socket

    try:
        if not connection.address:
            return False

        connection_port = connection.port or DEFAULT_SOCKET_PORT

        logging.debug("Sending logs to %s:%s", connection.address,
                      connection_port)

        if not isinstance(connection_port, int):
            connection_port = int(connection_port)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(connection.deadline)
        s.connect((connection.address, connection_port))

        # Add url and request id as a key
        s.sendall(payload)
        s.close()

        return True

    except Exception, e:
        logging.debug(e)
        return False
