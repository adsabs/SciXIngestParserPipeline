import contextlib
from unittest import TestCase


class base_utils(TestCase):
    @staticmethod
    @contextlib.contextmanager
    def mock_multiple_targets(mock_patches):
        """
        `mock_patches` is a list (or iterable) of mock.patch objects
        This is required when too many patches need to be applied in a nested
        `with` statement, since python has a hardcoded limit (~20).
        Based on: https://gist.github.com/msabramo/dffa53e4f29ec2e3682e
        """
        mocks = {}

        for mock_name, mock_patch in mock_patches.items():
            _mock = mock_patch.start()
            mocks[mock_name] = _mock

        yield mocks

        for mock_name, mock_patch in mock_patches.items():
            mock_patch.stop()


class mock_job_request(object):
    def value(self):
        return {
            "hash": "g425897fh3qp35890u54256342ewferht242546",
            "task_args": {
                "ingest_type": "metadata",
                "daterange": "2023-03-07",
            },
            "task": "SYMBOL1",
        }
