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
import os
import unittest

from mock import patch

from furious.async import Async

from furious.extras.appengine.logger import log
from furious.extras.appengine.logger import log_async_info_external
from furious.extras.appengine.logger import _build_url
from furious.extras.appengine.logger import _get_env_info
from furious.extras.appengine.logger import _log_async_info_to_http
from furious.extras.appengine.logger import _log_async_info_to_socket


@patch('furious.extras.appengine.logger.json.dumps')
@patch('furious.extras.appengine.logger.log_async_info_external')
class LogTestCase(unittest.TestCase):

    def test_no_external_logger(self, log_external, json_dump):
        """Ensure no external logger configured does not call the external
        logger method.
        """
        async = Async('foo')

        headers = {
            'X-Appengine-Tasketa': 4,
            'X-Appengine-Taskretrycount': 3,
            'X-Appengine-Taskexecutioncount': 2,
        }

        with patch('furious.extras.appengine.logger.time.time') as time:
            time.return_value = 10

            log(async, headers, 200, {'start_time': 10})

        self.assertFalse(log_external.called)

        args, _ = json_dump.call_args
        task_info = args[0]

        self.assertEqual(task_info, {
            'ran': 10,
            'retry_count': 3,
            'gae_latency_seconds': 6.0,
            'task_eta': 4.0,
            'execution_count': 2,
            'end': 10,
            'run_time': 0
        })

    @patch('furious.extras.appengine.logger.get_config')
    def test_has_external_logger(self, get_config, log_external, json_dump):
        """Ensure the external logger configured calls the external logger
        method.
        """
        async = Async('foo')
        config = {'log_external': True}
        get_config.return_value = config

        headers = {
            'X-Appengine-Tasketa': 4,
            'X-Appengine-Taskretrycount': 3,
            'X-Appengine-Taskexecutioncount': 2,
        }

        with patch('furious.extras.appengine.logger.time.time') as time:
            time.return_value = 11

            log(async, headers, 200, {'start_time': 10})

        args, _ = json_dump.call_args
        task_info = args[0]

        log_external.assert_called_once_with(config, async, 200, task_info)


@patch('furious.extras.appengine.logger._log_async_info_to_http')
class LogAsyncInfoExternalTestCase(unittest.TestCase):

    def setUp(self):
        super(LogAsyncInfoExternalTestCase, self).setUp()

        self._id = 'asyncid'
        request_id = os.environ.get('REQUEST_LOG_ID', 'NO_REQUEST')

        self.async_info = {
            'url': 'foo',
            'request_info': "%s.%s.%s.%s#%s" % (
                _get_env_info('APPLICATION_ID', 'NO_APPID'),
                _get_env_info('CURRENT_VERSION_ID', 'NO_VERSION'),
                _get_env_info('CURRENT_MODULE_ID', 'NO_MODULE'),
                _get_env_info('INSTANCE_ID', 'NO_INSTANCE_ID'),
                request_id),
            'request_address': _get_env_info('APPLICATION_ID', 'NO_APPID'),
            'id': "%s:%s" % (request_id, self._id)
        }

        self.task_info = {
            'ran': 10,
            'retry_count': 3,
            'gae_latency_seconds': 6.0,
            'task_eta': 4.0,
            'execution_count': 2,
            'status_code': 200
        }

        self.async_info.update(self.task_info)

    def test_http_connection(self, log_http):
        """Ensure a http protocol triggers the http log method."""

        address = 'externaladdr'
        protocol = 'http'

        config = {
            'log_external_address': address,
            'log_external_protocol': protocol,
        }

        async = Async('foo', id=self._id)

        log_async_info_external(config, async, 200, self.task_info)

        payload = json.dumps(self.async_info)

        log_http.assert_called_once_with(address, None, 2, payload)

    def test_default_connection(self, log_http):
        """Ensure no protocol triggers the http log method."""

        address = 'externaladdr'

        config = {
            'log_external_address': address,
        }

        async = Async('foo', id=self._id)

        log_async_info_external(config, async, 200, self.task_info)

        payload = json.dumps(self.async_info)

        log_http.assert_called_once_with(address, None, 2, payload)

    @patch('furious.extras.appengine.logger._log_async_info_to_socket')
    def test_tcp_connection(self, log_socket, log_http):
        """Ensure a tcp connection triggers the socket log method."""

        address = 'externaladdr'
        protocol = 'tcp'

        config = {
            'log_external_address': address,
            'log_external_protocol': protocol,
        }

        async = Async('foo', id=self._id)

        log_async_info_external(config, async, 200, self.task_info)

        payload = json.dumps(self.async_info)

        self.assertFalse(log_http.called)
        log_socket.assert_called_once_with(protocol, address, None, 2, payload)

    @patch('furious.extras.appengine.logger._log_async_info_to_socket')
    def test_udp_connection(self, log_socket, log_http):
        """Ensure a upd connection triggers the socket log method."""

        address = 'externaladdr'
        protocol = 'udp'

        config = {
            'log_external_address': address,
            'log_external_protocol': protocol,
            'log_external_port': 123,
            'log_external_deadline': 3
        }

        async = Async('foo', id=self._id)

        log_async_info_external(config, async, 200, self.task_info)

        payload = json.dumps(self.async_info)

        self.assertFalse(log_http.called)
        log_socket.assert_called_once_with(protocol, address, 123, 3, payload)

    @patch('furious.extras.appengine.logger._log_async_info_to_socket')
    def test_no_connection_address(self, log_socket, log_http):
        """Ensure no connection address returns with no action taken."""

        protocol = 'udp'

        config = {
            'log_external_protocol': protocol,
            'log_external_port': 123,
            'log_external_deadline': 3
        }

        async = Async('foo', id=self._id)

        log_async_info_external(config, async, 200, self.task_info)

        self.assertFalse(log_http.called)
        self.assertFalse(log_socket.called)

    @patch('furious.extras.appengine.logger._log_async_info_to_socket')
    def test_no_payload(self, log_socket, log_http):
        """Ensure no payload to pass returns with no action taken."""

        address = 'externaladdr'
        protocol = 'udp'

        config = {
            'log_external_address': address,
            'log_external_protocol': protocol,
            'log_external_port': 123,
            'log_external_deadline': 3
        }

        payload = json.dumps({
            'request_info': "%s.%s.%s.%s#%s" % (
                _get_env_info('APPLICATION_ID', 'NO_APPID'),
                _get_env_info('CURRENT_VERSION_ID', 'NO_VERSION'),
                _get_env_info('CURRENT_MODULE_ID', 'NO_MODULE'),
                _get_env_info('INSTANCE_ID', 'NO_INSTANCE_ID'),
                os.environ.get('REQUEST_LOG_ID', 'NO_REQUEST')
            ),
            'request_address': _get_env_info('APPLICATION_ID', 'NO_APPID'),
            'status_code': 200
        })

        log_async_info_external(config, None, 200)

        self.assertFalse(log_http.called)
        log_socket.assert_called_once_with(protocol, address, 123, 3, payload)

    @patch('furious.extras.appengine.logger._log_async_info_to_socket')
    def test_failed_payload(self, log_socket, log_http):
        """Ensure a failed payload to pass returns with no action taken."""
        from decimal import Decimal

        address = 'externaladdr'
        protocol = 'udp'

        config = {
            'log_external_address': address,
            'log_external_protocol': protocol,
            'log_external_port': 123,
            'log_external_deadline': 3
        }

        log_async_info_external(config, {'foo': Decimal('10')}, 200)

        self.assertFalse(log_http.called)
        self.assertFalse(log_socket.called)


@patch('google.appengine.api.urlfetch.fetch')
class LogAsyncInfoToHttpTestCase(unittest.TestCase):

    def test_successful_request(self, url_fetch):
        """Ensure a successful request does not raise and returns True."""

        address = "localhost"
        port = None
        deadline = 1
        payload = json.dumps({"info": "test"})

        url_fetch.return_value.status_code = 200

        result = _log_async_info_to_http(address, port, deadline, payload)

        url_fetch.assert_called_once_with(
            'http://localhost', deadline=deadline, payload=payload,
            method=2)

        self.assertTrue(result)

    def test_unsuccessful_request(self, url_fetch):
        """Ensure an unsuccessful request does not raise and returns False."""

        address = "localhost"
        port = None
        deadline = 1
        payload = json.dumps({"info": "test"})

        url_fetch.return_value.status_code = 500

        result = _log_async_info_to_http(address, port, deadline, payload)

        url_fetch.assert_called_once_with(
            'http://localhost', deadline=deadline, payload=payload,
            method=2)

        self.assertFalse(result)

    def test_error_request(self, url_fetch):
        """Ensure an error request does not raise and returns False."""

        address = "localhost"
        port = None
        deadline = 1
        payload = json.dumps({"info": "test"})

        url_fetch.side_effect = Exception()

        result = _log_async_info_to_http(address, port, deadline, payload)

        url_fetch.assert_called_once_with(
            'http://localhost', deadline=deadline, payload=payload,
            method=2)

        self.assertFalse(result)


@patch('socket.socket')
class LogAsyncInfoToSocketTestCase(unittest.TestCase):

    def test_success_with_port(self, socket):
        """Ensure the conneciton is made with the passed in port."""
        protocol = "tcp"
        address = "localhost"
        port = 1200
        deadline = 1
        payload = json.dumps({"info": "test"})

        result = _log_async_info_to_socket(protocol, address, port, deadline,
                                           payload)

        self.assertTrue(result)

        socket.assert_called_once_with(2, 1)
        socket.return_value.settimeout(deadline)
        socket.return_value.connect.assert_called_once_with((address, port))
        socket.return_value.sendall.assert_called_once_with(payload)
        socket.return_value.close.assert_called_once_with()

    def test_success_with_non_int_port(self, socket):
        """Ensure the conneciton is made with the passed in port converted to
        an int.
        """
        protocol = "tcp"
        address = "localhost"
        port = "1200"
        deadline = 1
        payload = json.dumps({"info": "test"})

        result = _log_async_info_to_socket(protocol, address, port, deadline,
                                           payload)

        self.assertTrue(result)

        socket.assert_called_once_with(2, 1)
        socket.return_value.settimeout(deadline)
        socket.return_value.connect.assert_called_once_with((address, 1200))
        socket.return_value.sendall.assert_called_once_with(payload)
        socket.return_value.close.assert_called_once_with()

    def test_success_with_no_port_passed_in(self, socket):
        """Ensure the conneciton is made with the default port."""
        protocol = "udp"
        address = "localhost"
        deadline = 1
        payload = json.dumps({"info": "test"})

        result = _log_async_info_to_socket(protocol, address, None, deadline,
                                           payload)

        self.assertTrue(result)

        socket.assert_called_once_with(2, 1)
        socket.return_value.settimeout(deadline)
        socket.return_value.connect.assert_called_once_with((address, 1300))
        socket.return_value.sendall.assert_called_once_with(payload)
        socket.return_value.close.assert_called_once_with()

    def test_error(self, socket):
        """Ensure False is returned and no exception is raised in the error
        case.
        """
        protocol = "tcp"
        address = "localhost"
        deadline = 1
        payload = json.dumps({"info": "test"})

        socket.return_value.connect.side_effect = Exception()

        result = _log_async_info_to_socket(protocol, address, None, deadline,
                                           payload)

        self.assertFalse(result)

        socket.assert_called_once_with(2, 1)
        socket.return_value.settimeout(deadline)
        socket.return_value.connect.assert_called_once_with((address, 1300))
        self.assertFalse(socket.return_value.sendall.called)
        self.assertFalse(socket.return_value.close.called)


class BuildUrlTestCase(unittest.TestCase):

    def test_no_http_with_trailing_slash_with_port(self):
        """Ensure an address with no http protocol a trailing slash and port
        are correctly put together.
        """
        address = "google.com/"
        port = 80

        result = _build_url(address, port)

        self.assertEqual(result, "http://google.com:80")

    def test_no_http_with_no_trailing_slash_with_port(self):
        """Ensure an address with no http protocol, no trailing slash and port
        are correctly put together.
        """
        address = "google.com"
        port = 80

        result = _build_url(address, port)

        self.assertEqual(result, "http://google.com:80")

    def test_no_http_with_no_trailing_slash_no_port(self):
        """Ensure an address with no http protocol, no trailing slash and no
        port are correctly put together.
        """
        address = "google.com"

        result = _build_url(address, None)

        self.assertEqual(result, "http://google.com")

    def test_http_with_no_trailing_slash_no_port(self):
        """Ensure an address with a http protocol, no trailing slash and no
        port are correctly put together.
        """
        address = "http://google.com"

        result = _build_url(address, None)

        self.assertEqual(result, "http://google.com")

    def test_https_with_no_trailing_slash_no_port(self):
        """Ensure an address with a https protocol, no trailing slash and no
        port are correctly put together.
        """
        address = "https://google.com"

        result = _build_url(address, None)

        self.assertEqual(result, "https://google.com")

    def test_http_with_port_as_part_of_address(self):
        """Ensure an address with the port as the address is correctly
        returned.
        """
        address = "http://google.com:80"

        result = _build_url(address, None)

        self.assertEqual(result, "http://google.com:80")

    def test_http_with_port_as_part_of_address_and_port_passed_in(self):
        """Ensure an address with the port as the address and with a port
        passed in is correctly returned.
        """
        address = "http://google.com:80"

        result = _build_url(address, 90)

        self.assertEqual(result, "http://google.com:80")

    def test_no_http_with_port_as_part_of_address_and_port_passed_in(self):
        """Ensure an address with the port as the address and with a port
        passed in is correctly returned.
        """
        address = "google.com:80"

        result = _build_url(address, 90)

        self.assertEqual(result, "http://google.com:80")
