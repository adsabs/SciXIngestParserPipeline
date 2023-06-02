import contextlib
from unittest import TestCase


class mock_gRPC_avro_msg:
    def value(self):
        return {
            "record_id": "g425897fh3qp35890u54256342ewferht242546",
            "task": "SYMBOL1",
            "status": None,
            "task_args": {
                "ingest": None,
                "ingest_type": "metadata",
                "daterange": "2023-03-07",
                "persistence": None,
            },
        }

    def bitstream(self):
        return b"\x00Ng425897fh3qp35890u54256342ewferht242546\x02\x00\x00\x02\x10metadata\x02\x142023-03-07\x02"


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
