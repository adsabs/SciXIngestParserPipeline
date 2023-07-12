import contextlib
import json
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
    def __init__(self, source="ARXIV"):
        self.record_id = uuid.uuid4()
        self.source = source

    def value(self):
        with open("SciXParser/tests/stubdata/arxiv_raw_xml_data.xml", "r") as f:
            record_metadata = f.read()
            return {
                "record_id": self.record_id,
                "record_xml": record_metadata,
                "s3_path": "/{}".format(self.record_id),
                "task": self.source,
                "datetime": datetime.now(),
            }


class mock_reparse_job_request(object):
    def __init__(self, force=False, resend=False):
        self.record_id = uuid.uuid4()
        self.force = force
        self.resend = resend

    def value(self):
        return {
            "record_id": self.record_id,
            "task": "REPARSE",
            "force": self.force,
            "resend": self.resend,
        }


class bad_producer(object):
    def produce(*args, **kwargs):
        raise ValueError


class mock_reparse_db_entry(object):
    def __init__(self, record_id, s3_key, record=None):
        self.id = record_id
        self.s3_key = s3_key
        self.date_created = datetime(2023, 6, 1)
        self.date_modifed = None
        self.parsed_data = record
        self.source = "ARXIV"


class mock_avro_test(object):
    def value(self):
        with open("SciXParser/tests/stubdata/arxiv_parsed_data.json") as f:
            msg_json = json.load(f)
        full_dict = {
            "parsed_record": msg_json,
            "record_id": "206f479f-bb1e-49ff-96df-491d66769abc",
        }
        return full_dict

    def deserialized_value(self):
        with open("SciXParser/tests/stubdata/arxiv_deserialized_parsed_data.json") as f:
            msg_json = json.load(f)
        full_dict = {
            "parsed_record": msg_json,
            "record_id": "206f479f-bb1e-49ff-96df-491d66769abc",
        }
        return full_dict

    def bitstream(self):
        return b'H206f479f-bb1e-49ff-96df-491d66769abc\x00\x00\x00\x0062023-06-23T13:49:50.791911Z\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x02\x142021-07-22\x00\x00\x00\x00\x02.eprint arXiv:2107.10460\x00\x00\x00\x00\x00\x00\x00\x00\x02\x082021\x00\x00\x00\x02\x02\x00\x00\x00\x02410.1038/s41535-022-00470-6\x02\x02\narXiv\x02\x142107.10460\x00\x00\x00\x02\x10\x00\x02\x02\x12Terashima\x02\x0cTaichi\x00\x00\x00\x02"Terashima, Taichi\x00\x00\x00\x00\x00\x02\x02\x0cHirose\x02\x0eHishiro\x02\x04T.\x00\x00\x02$Hirose, Hishiro T.\x00\x00\x00\x00\x00\x02\x02\x10Kikugawa\x02\nNaoki\x00\x00\x00\x02\x1eKikugawa, Naoki\x00\x00\x00\x00\x00\x02\x02\x06Uji\x02\x0cShinya\x00\x00\x00\x02\x16Uji, Shinya\x00\x00\x00\x00\x00\x02\x02\x08Graf\x02\nDavid\x00\x00\x00\x02\x16Graf, David\x00\x00\x00\x00\x00\x02\x02\x10Morinari\x02\nTakao\x00\x00\x00\x02\x1eMorinari, Takao\x00\x00\x00\x00\x00\x02\x02\x08Wang\x02\x08Teng\x00\x00\x00\x02\x14Wang, Teng\x00\x00\x00\x00\x00\x02\x02\x04Mu\x02\x08Gang\x00\x00\x00\x02\x10Mu, Gang\x00\x00\x00\x00\x00\x00\x00\x02\x9e\x01Anomalous High-Field Magnetotransport in CaFeAsF due to the Quantum Hall Effect\x00\x00\x00\x02\x02\xb8\rCaFeAsF is an iron-based superconductor parent compound whose Fermi surface is quasi-two dimensional, composed of Dirac-electron and Schr\\"odinger-hole cylinders elongated along the $c$ axis. We measured the longitudinal and Hall resistivities in CaFeAsF with the electrical current in the $ab$ plane in magnetic fields up to 45 T applied along the $c$ axis and obtained the corresponding conductivities via tensor inversion. We found that both the longitudinal and Hall conductivities approached zero above $\\sim$40 T as the temperature was lowered to 0.4 K. Our analysis indicates that the Landau-level filling factor is $\\nu$ = 2 for both electrons and holes at these high field strengths, resulting in a total filling factor $\\nu$ = $\\nu_{hole} - \\nu_{electron}$ = 0. We therefore argue that the $\\nu$ = 0 quantum Hall state emerges under these conditions.\x00\x00\x02\x02\x00\x02\narxiv\x02JComment: 18 pages, 1 table, 4 figures\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x06\x00\x02HCondensed Matter - Superconductivity\x02\narxiv\x00\x00\x02HCondensed Matter - Materials Science\x02\narxiv\x00\x00\x02`Condensed Matter - Strongly Correlated Electrons\x02\narxiv\x00\x00\x00\x00\x00\x00\x00'
