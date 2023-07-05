import json
import os
from unittest import TestCase

import base
import moto
from confluent_kafka.avro import AvroProducer
from mock import patch
from SciXPipelineUtils import utils

from parser.parser import PARSER_APP
from SciXParser.parser import db
from tests.common.mockschemaregistryclient import MockSchemaRegistryClient


@moto.mock_s3
class TestParser(TestCase):
    def test_parser_task(self):
        mock_job_request = base.mock_job_request
        url = "https://test.bucket.domain"
        with open("SciXParser/tests/stubdata/AVRO_schemas/ParserOutputSchema.avsc") as f:
            schema_str = f.read()
        with patch.dict(os.environ, {"MOTO_S3_CUSTOM_ENDPOINTS": url}):
            with moto.mock_s3() and base.base_utils.mock_multiple_targets(
                {
                    "get_schema": patch.object(
                        utils,
                        "get_schema",
                        return_value=schema_str,
                    )
                }
            ):
                mock_app = PARSER_APP(proj_home="SciXParser/tests/stubdata/")
                mock_app.schema_client = MockSchemaRegistryClient()
                mock_app._init_logger()
                producer = AvroProducer({}, schema_registry=mock_app.schema_client)
                mock_app.parser_task(mock_job_request(), producer)

    def test_reparse_task(self):
        mock_job_request = base.mock_reparse_job_request(force=False, resend=False)
        url = "https://test.bucket.domain"
        with open("SciXParser/tests/stubdata/AVRO_schemas/ParserOutputSchema.avsc") as f:
            schema_str = f.read()

        with patch.dict(os.environ, {"MOTO_S3_CUSTOM_ENDPOINTS": url}):
            with moto.mock_s3() and base.base_utils.mock_multiple_targets(
                {
                    "get_schema": patch.object(
                        utils,
                        "get_schema",
                        return_value=schema_str,
                    ),
                    "get_parser_record": patch.object(
                        db,
                        "get_parser_record",
                        return_value=base.mock_reparse_db_entry(
                            str(mock_job_request.record_id),
                            "/{}".format(mock_job_request.record_id),
                        ),
                    ),
                }
            ):
                mock_app = PARSER_APP(proj_home="SciXParser/tests/stubdata/")
                mock_app.schema_client = MockSchemaRegistryClient()
                mock_app._init_logger()
                producer = AvroProducer({}, schema_registry=mock_app.schema_client)
                mock_app.parser_task(mock_job_request, producer)

    def test_reparse_task_force(self):
        mock_job_request = base.mock_reparse_job_request(force=True, resend=False)
        url = "https://test.bucket.domain"
        with open("SciXParser/tests/stubdata/AVRO_schemas/ParserOutputSchema.avsc") as f:
            schema_str = f.read()
        with open("SciXParser/tests/stubdata/arxiv_parsed_data.json", "r") as f:
            parsed_record = json.load(f)
        with patch.dict(os.environ, {"MOTO_S3_CUSTOM_ENDPOINTS": url}):
            with moto.mock_s3() and base.base_utils.mock_multiple_targets(
                {
                    "get_schema": patch.object(
                        utils,
                        "get_schema",
                        return_value=schema_str,
                    ),
                    "get_parser_record": patch.object(
                        db,
                        "get_parser_record",
                        return_value=base.mock_reparse_db_entry(
                            str(mock_job_request.record_id),
                            "/{}".format(mock_job_request.record_id),
                            parsed_record,
                        ),
                    ),
                }
            ):
                mock_app = PARSER_APP(proj_home="SciXParser/tests/stubdata/")
                mock_app.schema_client = MockSchemaRegistryClient()
                mock_app._init_logger()
                producer = AvroProducer({}, schema_registry=mock_app.schema_client)
                mock_app.parser_task(mock_job_request, producer)

    def test_reparse_task_resend(self):
        mock_job_request = base.mock_reparse_job_request(force=False, resend=True)
        url = "https://test.bucket.domain"
        with open("SciXParser/tests/stubdata/AVRO_schemas/ParserOutputSchema.avsc") as f:
            schema_str = f.read()
        with open("SciXParser/tests/stubdata/arxiv_parsed_data.json", "r") as f:
            parsed_record = json.load(f)
        with patch.dict(os.environ, {"MOTO_S3_CUSTOM_ENDPOINTS": url}):
            with moto.mock_s3() and base.base_utils.mock_multiple_targets(
                {
                    "get_schema": patch.object(
                        utils,
                        "get_schema",
                        return_value=schema_str,
                    ),
                    "get_parser_record": patch.object(
                        db,
                        "get_parser_record",
                        return_value=base.mock_reparse_db_entry(
                            str(mock_job_request.record_id),
                            "/{}".format(mock_job_request.record_id),
                            parsed_record,
                        ),
                    ),
                }
            ):
                mock_app = PARSER_APP(proj_home="SciXParser/tests/stubdata/")
                mock_app.schema_client = MockSchemaRegistryClient()
                mock_app._init_logger()
                producer = AvroProducer({}, schema_registry=mock_app.schema_client)
                mock_app.parser_task(mock_job_request, producer)
