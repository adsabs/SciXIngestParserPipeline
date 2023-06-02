import contextlib
import uuid
from datetime import datetime
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
    def __init__(self):
        self.record_id = uuid.uuid4()

    def value(self):
        with open("SciXParser/tests/stubdata/arxiv_raw_xml_data.xml", "r") as f:
            record_metadata = f.read()
            return {
                "record_id": self.record_id,
                "record_xml": record_metadata,
                "s3_path": "/{}".format(self.record_id),
                "task": "ARXIV",
                "datetime": datetime.now(),
            }
