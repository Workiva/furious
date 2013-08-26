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

import unittest

from mock import patch

from nose.plugins.attrib import attr


class TestSelectQueue(unittest.TestCase):
    """Ensure select_queue() works correctly."""

    def tearDown(self):
        from furious.context import _local
        _local._clear_context()

    def test_none(self):
        """Ensure that None is returned when the queue group is None."""
        from furious.async import select_queue

        actual = select_queue(None)

        self.assertEqual(actual, None)

    @patch('furious.queues.get_config')
    def test_invalid_queue_count(self, mock_get_config):
        """Ensure that an exception is raised when a bad queue count is given.
        """
        from furious.async import select_queue

        queue_group = 'foo-queue'

        mock_get_config.return_value = {'queue_counts':
                                        {queue_group: 0}}

        with self.assertRaises(Exception) as context:
            select_queue(queue_group)

        self.assertEqual(context.exception.message,
                         'Queue group must have at least 1 queue.')

    @patch('furious.queues.get_config')
    def test_invalid_queue_count_config(self, mock_get_config):
        """Ensure that an exception is raised when a bad queue count config is
        provided.
        """
        from furious.async import select_queue

        queue_group = 'foo-queue'

        mock_get_config.return_value = {'queue_counts': 'woops'}

        with self.assertRaises(Exception) as context:
            select_queue(queue_group)

        self.assertEqual(context.exception.message,
                         'queue_counts config property must be a dict.')

    @patch('furious.queues.get_config')
    @patch('furious.queues._get_from_cache')
    def test_random_from_cache(self, mock_cache, mock_get_config):
        """Ensure that a random queue is selected from the group when there are
        cached queues.
        """
        from furious.async import select_queue

        queue_group = 'foo-queue'
        queue_count = 5
        expected = '%s-4' % queue_group

        mock_get_config.return_value = {'queue_counts':
                                        {queue_group: queue_count}}

        mock_cache.return_value = {queue_group: [expected]}

        actual = select_queue(queue_group)

        self.assertEqual(actual, expected)
        mock_cache.assert_called_once_with('queues', default={})

    @patch('furious.queues.get_config')
    @patch('furious.queues.shuffle')
    @patch('furious.queues._get_from_cache')
    def test_random_no_cache(self, mock_cache, mock_shuffle, mock_get_config):
        """Ensure that a random queue is selected from the group when there are
        no cached queues.
        """
        from furious.async import select_queue

        queue_group = 'foo-queue'
        queue_count = 5
        expected = '%s-4' % queue_group

        mock_get_config.return_value = {'queue_counts':
                                        {queue_group: queue_count}}

        mock_cache.return_value = {}

        actual = select_queue(queue_group)

        self.assertEqual(actual, expected)
        mock_cache.assert_called_once_with('queues', default={})
        self.assertTrue(mock_shuffle.called)

    def test_get_from_cache_no_key(self):
        """Ensure the default value is returned when the key is None."""
        from furious.queues import _get_from_cache

        key = None
        expected = 'default'

        actual = _get_from_cache(key, default=expected)

        self.assertEqual(actual, expected)

    @attr('slow')
    def test_get_from_cache_present(self):
        """Ensure the correct value is returned when the key is present."""
        from furious.context._local import get_local_context
        from furious.queues import _get_from_cache

        key = 'key'
        expected = 'value'
        setattr(get_local_context(), key, expected)

        actual = _get_from_cache(key)

        self.assertEqual(actual, expected)

    @attr('slow')
    def test_get_from_cache_not_present(self):
        """Ensure the default value is returned and set when the key is not
        present.
        """
        from furious.context._local import get_local_context
        from furious.queues import _get_from_cache

        key = 'key'
        expected = 'default'

        actual = _get_from_cache(key, default=expected)

        self.assertEqual(actual, expected)
        self.assertTrue(hasattr(get_local_context(), key))

