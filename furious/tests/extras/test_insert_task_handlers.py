import unittest

from google.appengine.api import taskqueue
from google.appengine.ext import testbed
from mock import Mock
from mock import patch

from furious.extras.insert_task_handlers import \
    insert_tasks_ignore_duplicate_names


class TestInsertTasksIgnoreDuplicateNames(unittest.TestCase):
    """Test that _insert_tasks_ignore_duplicate_names behaves as expected."""
    def setUp(self):
        harness = testbed.Testbed()
        harness.activate()
        harness.init_taskqueue_stub()

    @patch('furious.extras.insert_task_handlers._insert_tasks')
    def test_insert_tasks_ignore_duplicate_names_relays_parms(self,
                                                              mock_tasks):
        """Ensure args and kwargs passed, are passed onto _insert_tasks as
        expected.
        """

        insert_tasks_ignore_duplicate_names(['1'], 'queue', 'arg1', foo='bar')

        mock_tasks.assert_called_once_with(['1'], 'queue', 'arg1', foo='bar')

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_insert_tasks_ignore_duplicate_names_raises(self, mock_add):
        """Ensure if `DuplicateTaskNameError` raised, that each task is
        retried.

        Also ensure that TaskAlreadyExistsError is not re-raised on the
        subsequent retries.
        """

        mock_add.side_effect = (taskqueue.DuplicateTaskNameError,
                                taskqueue.TaskAlreadyExistsError,
                                taskqueue.TaskAlreadyExistsError,
                                taskqueue.TaskAlreadyExistsError)

        tasks = [taskqueue.Task('1'), taskqueue.Task('2'), taskqueue.Task('3')]
        inserted = insert_tasks_ignore_duplicate_names(tasks, 'queue')

        # Ensure an attempt to re-insert all tasks.
        self.assertEqual(inserted, 0)
        self.assertEqual(mock_add.call_count, 4)
        mock_add.assert_any_call(tasks, transactional=False)
        for task in tasks:
            mock_add.assert_any_call([task], transactional=False)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_insert_tasks_ignore_duplicate_names_partial_errors(self, mock_add):
        """Ensure if `DuplicateTaskNameError` raised, that each task is
        retried, and count is correct if some tasks could be re-inserted.
        """

        # Bulk add fails, then 2nd named task fails.
        mock_add.side_effect = (taskqueue.DuplicateTaskNameError,
                                None,
                                taskqueue.TaskAlreadyExistsError,
                                None)

        tasks = [taskqueue.Task('1'), taskqueue.Task('2'), taskqueue.Task('3')]
        inserted = insert_tasks_ignore_duplicate_names(tasks, 'queue')

        self.assertEqual(inserted, 2)
        self.assertEqual(mock_add.call_count, 4)
        mock_add.assert_any_call(tasks, transactional=False)
        for task in tasks:
            mock_add.assert_any_call([task], transactional=False)

    @patch('google.appengine.api.taskqueue.Queue.add', auto_spec=True)
    def test_insert_tasks_ignore_duplicate_names_was_enqueued(self, mock_add):
        """Ensure only tasks that were not enqueued on the initial BulkAdd() are
        re-inserted.
        """

        # Bulk add fails, then 2nd named task fails.
        mock_add.side_effect = (taskqueue.DuplicateTaskNameError,
                                taskqueue.TaskAlreadyExistsError,
                                None)
        tasks = [Mock(), taskqueue.Task('2'), taskqueue.Task('3')]
        tasks[0].was_enqueued = True

        inserted = insert_tasks_ignore_duplicate_names(tasks, 'queue')

        self.assertEqual(inserted, 2)
        self.assertEqual(mock_add.call_count, 3)

        # No attempt to re-insert tasks[0], it was marked as 'enqueued'.
        mock_add.assert_any_call(tasks, transactional=False)
        mock_add.assert_any_call([tasks[1]], transactional=False)
        mock_add.assert_any_call([tasks[2]], transactional=False)
