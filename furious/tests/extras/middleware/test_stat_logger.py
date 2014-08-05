import unittest

from mock import ANY
from mock import MagicMock
from mock import Mock
from mock import patch


from furious.extras.middleware.stat_logger import furious_wsgi_middleware
from furious.extras.middleware.stat_logger import log_stats
from furious.extras.middleware.stat_logger import StatConnection


@patch('furious.extras.middleware.stat_logger.log_stats')
class FuriousWsgiMiddlewareTestCase(unittest.TestCase):

    def test_200_with_result(self, logger):
        """Ensure a 200 run logs the stats, yields the results, closes the
        result and sets a 200 status code.
        """
        app_result = iter([1, 2])
        app = Mock(return_value=app_result)
        env = {}
        start_response = Mock()

        wrapper = furious_wsgi_middleware(app)

        result = wrapper(env, start_response)

        self.assertEqual([1, 2], list(result))

        app.assert_called_once_with(env, start_response)

        self.assertEqual(logger.call_args[0][0].status_code, 200)

        logger.assert_called_once_with(ANY)

    def test_200_with_no_result(self, logger):
        """Ensure a 200 run logs the stats, yields the results, closes the
        result and sets a 200 status code.
        """
        app_result = MagicMock(spec=list)
        app_result.close = Mock()
        app = Mock(return_value=app_result)
        env = {}
        start_response = Mock()

        wrapper = furious_wsgi_middleware(app)

        result = wrapper(env, start_response)

        self.assertEqual([], list(result))

        app.assert_called_once_with(env, start_response)

        self.assertEqual(logger.call_args[0][0].status_code, 200)

        logger.assert_called_once_with(ANY)
        app_result.close.assert_called_once_with()

    def test_500_with_result(self, logger):
        """Ensure a 500 run logs the stats, log the stats but then raises
        the failure.
        """
        app = Mock(side_effect=ValueError)
        env = {}
        start_response = Mock()

        wrapper = furious_wsgi_middleware(app)

        self.assertRaises(ValueError, list, wrapper(env, start_response))

        app.assert_called_once_with(env, start_response)

        self.assertEqual(logger.call_args[0][0].status_code, 500)

        logger.assert_called_once_with(ANY)


@patch('furious.extras.middleware.stat_logger.StatConnection')
class LogAsyncInfoExternalTestCase(unittest.TestCase):

    def setUp(self):
        super(LogAsyncInfoExternalTestCase, self).setUp()

        self.payload = {
            'foo': 'bar'
        }

        self.recorder = Mock()

    @patch('urllib2.urlopen')
    def test_http_connection(self, urlopen, connection):
        """Ensure a http protocol triggers the http log method."""

        self.recorder.get_payload.return_value = self.payload

        connection.return_value.address = "host"
        connection.return_value.protocol = "http"
        connection.return_value.deadline = 1

        log_stats(self.recorder)

        urlopen.assert_called_once_with('host', data=self.payload,
                                        timeout=1)

    @patch('urllib2.urlopen')
    def test_http_connection_failed(self, urlopen, connection):
        """Ensure a http protocol triggers the http log method and handles
        the failure.
        """
        urlopen.side_effect = ValueError

        self.recorder.get_payload.return_value = self.payload

        connection.return_value.address = "host"
        connection.return_value.protocol = "http"
        connection.return_value.deadline = 1

        log_stats(self.recorder)

        urlopen.assert_called_once_with('host', data=self.payload,
                                        timeout=1)

    @patch('urllib2.urlopen')
    def test_http_connection_with_no_url(self, urlopen, connection):
        """Ensure a http protocol with no url doesn't trigger the http log
        method.
        """

        self.recorder.get_payload.return_value = self.payload
        connection.return_value.address = None
        connection.return_value.protocol = "http"
        connection.return_value.deadline = 1

        log_stats(self.recorder)

        self.assertFalse(urlopen.called)

    @patch('urllib2.urlopen')
    def test_http_connection_with_no_payload(self, urlopen, connection):
        """Ensure a http protocol with no payload doesn't trigger the http log
        method.
        """

        self.recorder.get_payload.return_value = None
        connection.return_value.address = "host"
        connection.return_value.protocol = "http"
        connection.return_value.deadline = 1

        log_stats(self.recorder)

        self.assertFalse(urlopen.called)

    @patch('socket.socket')
    def test_socket_connection(self, socket, connection):
        """Ensure a socket protocol triggers the socket log method."""

        self.recorder.get_payload.return_value = self.payload
        connection.return_value.address = "host"
        connection.return_value.protocol = "tcp"
        connection.return_value.deadline = 1

        log_stats(self.recorder)

        socket.assert_called_once_with(2, 1)
        socket.return_value.settimeout.assert_called_once_with(1)
        socket.return_value.connect.assert_called_once_with(('host', 1))
        socket.return_value.sendall.assert_called_once_with(self.payload)
        socket.return_value.close.assert_called_once_with()

    @patch('socket.socket')
    def test_socket_connection_fails(self, socket, connection):
        """Ensure a socket protocol triggers the socket log method and handles
        the failure.
        """
        socket.return_value.connect.side_effect = ValueError

        self.recorder.get_payload.return_value = self.payload
        connection.return_value.address = "host"
        connection.return_value.protocol = "tcp"
        connection.return_value.deadline = 1

        log_stats(self.recorder)

        socket.assert_called_once_with(2, 1)
        socket.return_value.settimeout.assert_called_once_with(1)
        socket.return_value.connect.assert_called_once_with(('host', 1))
        self.assertFalse(socket.return_value.sendall.called)
        self.assertFalse(socket.return_value.close.called)

    @patch('socket.socket')
    def test_socket_connection_with_no_host(self, socket, connection):
        """Ensure a socket protocol with no host doesn't trigger the socket log
        method.
        """

        self.recorder.get_payload.return_value = self.payload
        connection.return_value.address = None
        connection.return_value.protocol = "tcp"
        connection.return_value.deadline = 1

        log_stats(self.recorder)

        self.assertFalse(socket.called)

    @patch('socket.socket')
    def test_socket_connection_with_no_payload(self, socket, connection):
        """Ensure a socket protocol with no payload doesn't trigger the socket
        log method.
        """

        self.recorder.get_payload.return_value = None
        connection.return_value.address = "host"
        connection.return_value.protocol = "tcp"
        connection.return_value.deadline = 1

        log_stats(self.recorder)

        self.assertFalse(socket.called)

    @patch('socket.socket')
    def test_connection_with_no_protocol(self, socket, connection):
        """Ensure an address with no protocol triggers the default socket
        method.
        """

        self.recorder.get_payload.return_value = self.payload
        connection.return_value.address = "host"
        connection.return_value.protocol = None
        connection.return_value.deadline = 1
        connection.return_value.port = 100

        log_stats(self.recorder)

        socket.assert_called_once_with(2, 1)
        socket.return_value.settimeout.assert_called_once_with(1)
        socket.return_value.connect.assert_called_once_with(('host', 100))
        socket.return_value.sendall.assert_called_once_with(self.payload)
        socket.return_value.close.assert_called_once_with()


@patch('furious.extras.middleware.stat_logger.get_config')
class StatConnectionTestCase(unittest.TestCase):

    def test_stat_connection_loads_config(self, get_config):
        """Ensure if no config is passed in it loads the config."""
        cfg = {
            'stat_logger_connection': {
                'address': 'addr',
                'protocol': 'proto',
                'port': 90,
                'deadline': 5
            }
        }

        get_config.return_value = cfg

        connection = StatConnection(config=None)

        self.assertEqual(connection._config, cfg)

        self.assertEqual(connection.address, 'addr')
        self.assertEqual(connection.protocol, 'proto')
        self.assertEqual(connection.port, 90)
        self.assertEqual(connection.deadline, 5)
