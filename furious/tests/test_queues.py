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


class TestSelectQueue(unittest.TestCase):
    """Ensure select_queue() works correctly."""

    def setUp(self):
        from threading import local
        from furious import async

        async._cache = local()

    def test_none(self):
        """Ensure that None is returned when the queue group is None."""
        from furious.async import select_queue

        actual = select_queue(None)

        self.assertEqual(actual, None)

    def test_invalid_queue_count(self):
        """Ensure that an exception is raised when a bad queue count is given.
        """
        from furious.async import select_queue

        with self.assertRaises(Exception) as context:
            select_queue('foo-queue', queue_count=0)

        self.assertEqual(context.exception.message,
                         'Queue group must have at least 1 queue.')

    @patch('furious.queues._get_from_cache')
    def test_random_from_cache(self, mock_cache):
        """Ensure that a random queue is selected from the group when there are
        cached queues.
        """
        from furious.async import select_queue

        queue_group = 'foo-queue'
        queue_count = 5
        expected = '%s-4' % queue_group

        mock_cache.return_value = {queue_group: [expected]}

        actual = select_queue(queue_group, queue_count=queue_count)

        self.assertEqual(actual, expected)
        mock_cache.assert_called_once_with('queues', default={})

    @patch('furious.queues.shuffle')
    @patch('furious.queues._get_from_cache')
    def test_random_no_cache(self, mock_cache, mock_shuffle):
        """Ensure that a random queue is selected from the group when there are
        no cached queues.
        """
        from furious.async import select_queue

        queue_group = 'foo-queue'
        queue_count = 5
        expected = '%s-4' % queue_group

        mock_cache.return_value = {}

        actual = select_queue(queue_group, queue_count=queue_count)

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

    def test_get_from_cache_present(self):
        """Ensure the correct value is returned when the key is present."""
        from furious.queues import _cache
        from furious.queues import _get_from_cache

        key = 'key'
        expected = 'value'
        setattr(_cache, key, expected)

        actual = _get_from_cache(key)

        self.assertEqual(actual, expected)

    def test_get_from_cache_not_present(self):
        """Ensure the default value is returned and set when the key is not
        present.
        """
        from furious.queues import _cache
        from furious.queues import _get_from_cache

        key = 'key'
        expected = 'default'

        actual = _get_from_cache(key, default=expected)

        self.assertEqual(actual, expected)
        self.assertTrue(hasattr(_cache, key))

