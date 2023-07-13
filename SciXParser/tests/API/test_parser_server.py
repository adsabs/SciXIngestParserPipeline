import json
import logging
import uuid
from concurrent import futures
from unittest import TestCase

import grpc
import pytest
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import Schema
from mock import patch
from SciXPipelineUtils.avro_serializer import AvroSerialHelper

from API.grpc_modules import parser_grpc
from API.parser_client import get_schema
from API.parser_server import Listener, Logging, initialize_parser
from SciXParser.parser import db
from tests.API import base
from tests.common.mockschemaregistryclient import MockSchemaRegistryClient


class fake_db_entry(object):
    def __init__(self, status="Success"):
        self.name = status


class ParserServer(TestCase):
    def setUp(self):
        """Instantiate a Parser server and return a stub for use in tests"""
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        self.logger = Logging(logging)
        self.schema_client = MockSchemaRegistryClient()
        self.VALUE_SCHEMA_FILE = "SciXParser/tests/stubdata/AVRO_schemas/ParserInputSchema.avsc"
        self.VALUE_SCHEMA_FILE_2 = "SciXParser/tests/stubdata/AVRO_schemas/ParserOutputSchema.avsc"
        self.VALUE_SCHEMA_NAME = "ParserInputSchema"
        self.VALUE_SCHEMA_NAME_2 = "ParserOutputSchema"
        self.value_schema = open(self.VALUE_SCHEMA_FILE).read()
        self.value_schema_2 = open(self.VALUE_SCHEMA_FILE_2).read()

        self.schema_client.register(self.VALUE_SCHEMA_NAME, Schema(self.value_schema, "AVRO"))
        self.ser_schema = get_schema(self.logger, self.schema_client, self.VALUE_SCHEMA_NAME)
        self.schema_client.register(self.VALUE_SCHEMA_NAME_2, Schema(self.value_schema_2, "AVRO"))
        self.des_schema = get_schema(self.logger, self.schema_client, self.VALUE_SCHEMA_NAME_2)
        self.avroserialhelper = AvroSerialHelper(
            ser_schema=self.ser_schema, logger=self.logger.logger
        )
        self.monavroserialhelper = AvroSerialHelper(
            ser_schema=self.des_schema, des_schema=self.ser_schema, logger=self.logger.logger
        )
        self.monclientavroserialhelper = AvroSerialHelper(
            ser_schema=self.ser_schema, des_schema=self.des_schema, logger=self.logger.logger
        )
        OUTPUT_VALUE_SCHEMA_FILE = "SciXParser/tests/stubdata/AVRO_schemas/ParserOutputSchema.avsc"
        OUTPUT_VALUE_SCHEMA_NAME = "ParserOutputSchema"
        output_value_schema = open(OUTPUT_VALUE_SCHEMA_FILE).read()

        self.schema_client.register(OUTPUT_VALUE_SCHEMA_NAME, Schema(output_value_schema, "AVRO"))
        self.producer = AvroProducer({}, schema_registry=MockSchemaRegistryClient())

        parser_grpc.add_ParserInitServicer_to_server(
            initialize_parser()(
                self.producer, self.ser_schema, self.schema_client, self.logger.logger
            ),
            self.server,
            self.avroserialhelper,
        )

        parser_grpc.add_ParserViewServicer_to_server(
            initialize_parser(parser_grpc.ParserViewServicer)(
                self.producer, self.ser_schema, self.schema_client, self.logger.logger
            ),
            self.server,
            self.monavroserialhelper,
        )

        parser_grpc.add_ParserMonitorServicer_to_server(
            initialize_parser(parser_grpc.ParserMonitorServicer)(
                self.producer, self.ser_schema, self.schema_client, self.logger.logger
            ),
            self.server,
            self.avroserialhelper,
        )
        self.port = 55551
        self.server.add_insecure_port(f"[::]:{self.port}")
        self.server.start()

    def tearDown(self):
        self.server.stop(None)

    def test_Parser_server_bad_entry(self):
        """
        An initial test to confirm gRPC raises an error if it is given an invalid message to serialize.
        input:
            s: AVRO message
        """
        s = {}

        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            stub = parser_grpc.ParserInitStub(channel, self.avroserialhelper)
            with pytest.raises(grpc.RpcError):
                stub.initParser(s)

    def test_Parser_server_init(self):
        """
        A test the of INIT method for the gRPC server
        input:
            s: AVRO message: ParserInputSchema
        """
        cls = initialize_parser()(
            self.producer, self.ser_schema, self.schema_client, self.logger.logger
        )
        record_id = str(uuid.uuid4())
        s = {"record_id": record_id, "status": "Error", "task": "ARXIV"}
        db.write_job_status(cls, s)
        s = {"record_id": record_id, "persistence": False, "task": "REPARSE"}
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            stub = parser_grpc.ParserInitStub(channel, self.avroserialhelper)
            responses = stub.initParser(s)
            for response in list(responses):
                self.assertEqual(response.get("status"), "Pending")
                self.assertNotEqual(response.get("record_id"), None)

    def test_Parser_server_init_multi_record(self):
        """
        A test the of INIT method for the gRPC server
        input:
            s: AVRO message: ParserInputSchema
        """
        cls = initialize_parser()(
            self.producer, self.ser_schema, self.schema_client, self.logger.logger
        )
        record_id = " ".join([str(uuid.uuid4()) for _ in range(0, 5)])
        for record in record_id.split():
            s = {"record_id": record, "status": "Error", "task": "ARXIV"}
            db.write_job_status(cls, s)

        s = {"record_id": record_id, "persistence": False, "task": "REPARSE"}
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            stub = parser_grpc.ParserInitStub(channel, self.avroserialhelper)
            responses = stub.initParser(s)
            for response in list(responses):
                self.assertEqual(response.get("status"), "Pending")
                self.assertNotEqual(response.get("record_id"), None)

    def test_Parser_server_init_persistence(self):
        """
        A test of the INIT method for the gRPC server with persistence
        input:
            s: AVRO message: ParserInputSchema
        """
        s = {"record_id": str(uuid.uuid4()), "persistence": True, "task": "REPARSE"}
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "update_job_status": patch.object(db, "update_job_status", return_value=True),
                    "get_job_status_by_record_id": patch.object(
                        db, "get_job_status_by_record_id", return_value=fake_db_entry("Processing")
                    ),
                    "__init__": patch.object(Listener, "__init__", return_value=None),
                    "subscribe": patch.object(Listener, "subscribe", return_value=True),
                    "get_status_redis": patch.object(
                        Listener, "get_status_redis", return_value=iter(["Success"])
                    ),
                }
            ):
                stub = parser_grpc.ParserInitStub(channel, self.avroserialhelper)
                responses = stub.initParser(s)
                final_response = []
                for response in list(responses):
                    self.assertNotEqual(response.get("record_id"), None)
                    final_response.append(response.get("status"))
                self.assertEqual(final_response, ["Pending", "Processing", "Success"])

    def test_Parser_server_init_persistence_error_redis(self):
        """
        A test of the INIT method for the gRPC server with persistence
        where an error is returned by the redis server.
        input:
            s: AVRO message: ParserInputSchema
        """
        s = {"record_id": str(uuid.uuid4()), "persistence": True, "task": "REPARSE"}
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "update_job_status": patch.object(db, "update_job_status", return_value=True),
                    "get_job_status_by_record_id": patch.object(
                        db, "get_job_status_by_record_id", return_value=fake_db_entry("Processing")
                    ),
                    "__init__": patch.object(Listener, "__init__", return_value=None),
                    "subscribe": patch.object(Listener, "subscribe", return_value=True),
                    "get_status_redis": patch.object(
                        Listener, "get_status_redis", return_value=iter(["Error"])
                    ),
                }
            ):
                stub = parser_grpc.ParserInitStub(channel, self.avroserialhelper)
                responses = stub.initParser(s)
                final_response = []
                for response in list(responses):
                    self.assertNotEqual(response.get("record_id"), None)
                    final_response.append(response.get("status"))
                self.assertEqual(final_response, ["Pending", "Processing", "Error"])

    def test_Parser_server_init_persistence_error_db(self):
        """
        A test of the INIT method for the gRPC server with persistence
        where an error is returned from postgres.
        input:
            s: AVRO message: ParserInputSchema
        """
        s = {
            "record_id": str(uuid.uuid4()),
            "persistence": True,
            "task": "REPARSE",
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "update_job_status": patch.object(db, "update_job_status", return_value=True),
                    "get_job_status_by_record_id": patch.object(
                        db, "get_job_status_by_record_id", return_value=fake_db_entry("Error")
                    ),
                    "__init__": patch.object(Listener, "__init__", return_value=None),
                    "subscribe": patch.object(Listener, "subscribe", return_value=True),
                    "get_status_redis": patch.object(
                        Listener, "get_status_redis", return_value=iter(["Error"])
                    ),
                }
            ):
                stub = parser_grpc.ParserInitStub(channel, self.avroserialhelper)
                responses = stub.initParser(s)
                final_response = []
                for response in list(responses):
                    self.assertNotEqual(response.get("record_id"), None)
                    final_response.append(response.get("status"))
                self.assertEqual(final_response, ["Pending", "Error"])

    def test_Parser_server_monitor(self):
        """
        A test of the MONITOR method for the gRPC server
        input:
            s: AVRO message: ParserInputSchema
        """
        s = {"task": "MONITOR", "record_id": str(uuid.uuid4()), "persistence": False}
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "get_job_status_by_record_id": patch.object(
                        db, "get_job_status_by_record_id", return_value=fake_db_entry()
                    )
                }
            ):
                stub = parser_grpc.ParserMonitorStub(channel, self.avroserialhelper)
                responses = stub.monitorParser(s)
                for response in list(responses):
                    self.assertEqual(response.get("status"), "Success")
                    self.assertEqual(response.get("record_id"), s.get("record_id"))

    def test_Parser_server_monitor_persistent_success(self):
        """
        A test of the MONITOR method for the gRPC server with persistence
        input:
            s: AVRO message: ParserInputSchema
        """
        s = {"task": "MONITOR", "record_id": str(uuid.uuid4()), "persistence": True}
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "update_job_status": patch.object(db, "update_job_status", return_value=True),
                    "get_job_status_by_record_id": patch.object(
                        db, "get_job_status_by_record_id", return_value=fake_db_entry("Success")
                    ),
                    "__init__": patch.object(Listener, "__init__", return_value=None),
                    "subscribe": patch.object(Listener, "subscribe", return_value=True),
                    "get_status_redis": patch.object(
                        Listener, "get_status_redis", return_value=iter(["Success"])
                    ),
                }
            ):
                stub = parser_grpc.ParserMonitorStub(channel, self.avroserialhelper)
                responses = stub.monitorParser(s)
                for response in list(responses):
                    self.assertEqual(response.get("status"), "Success")
                    self.assertEqual(response.get("hash"), s.get("hash"))

    def test_Parser_server_monitor_persistent_error_db(self):
        """
        A test of the MONITOR method for the gRPC server with persistence
        where an error is returned from postgres.
        input:
            s: AVRO message: ParserInputSchema
        """
        s = {"task": "MONITOR", "record_id": str(uuid.uuid4()), "persistence": True}
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "update_job_status": patch.object(db, "update_job_status", return_value=True),
                    "get_job_status_by_record_id": patch.object(
                        db, "get_job_status_by_record_id", return_value=fake_db_entry("Error")
                    ),
                    "__init__": patch.object(Listener, "__init__", return_value=None),
                    "subscribe": patch.object(Listener, "subscribe", return_value=True),
                    "get_status_redis": patch.object(
                        Listener, "get_status_redis", return_value=iter(["Success"])
                    ),
                }
            ):
                stub = parser_grpc.ParserMonitorStub(channel, self.avroserialhelper)
                responses = stub.monitorParser(s)
                for response in list(responses):
                    self.assertEqual(response.get("status"), "Error")
                    self.assertEqual(response.get("hash"), s.get("hash"))

    def test_Parser_server_monitor_persistent_error_redis(self):
        """
        A test of the MONITOR method for the gRPC server with persistence
        where an error is returned from redis.
        input:
            s: AVRO message: ParserInputSchema
        """
        s = {"task": "MONITOR", "record_id": str(uuid.uuid4()), "persistence": True}
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "update_job_status": patch.object(db, "update_job_status", return_value=True),
                    "get_job_status_by_record_id": patch.object(
                        db, "get_job_status_by_record_id", return_value=fake_db_entry("Processing")
                    ),
                    "__init__": patch.object(Listener, "__init__", return_value=None),
                    "subscribe": patch.object(Listener, "subscribe", return_value=True),
                    "get_status_redis": patch.object(
                        Listener, "get_status_redis", return_value=iter(["Error"])
                    ),
                }
            ):
                stub = parser_grpc.ParserMonitorStub(channel, self.avroserialhelper)
                responses = stub.monitorParser(s)
                final_responses = []
                for response in list(responses):
                    final_responses.append(response.get("status"))
                    self.assertEqual(response.get("hash"), s.get("hash"))
                self.assertEqual(final_responses, ["Processing", "Error"])

    def test_Parser_server_monitor_no_hash(self):
        """
        A test of the MONITOR method for the gRPC server with persistence
        where the job hash was not provided.
        input:
            s: AVRO message: ParserInputSchema
        """
        s = {"task": "MONITOR", "record_id": str(uuid.uuid4()), "persistence": True}
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "update_job_status": patch.object(db, "update_job_status", return_value=True),
                    "get_job_status_by_record_id": patch.object(
                        db, "get_job_status_by_record_id", return_value=fake_db_entry("Error")
                    ),
                    "__init__": patch.object(Listener, "__init__", return_value=None),
                    "subscribe": patch.object(Listener, "subscribe", return_value=True),
                    "get_status_redis": patch.object(
                        Listener, "get_status_redis", return_value=iter(["Success"])
                    ),
                }
            ):
                stub = parser_grpc.ParserMonitorStub(channel, self.avroserialhelper)
                responses = stub.monitorParser(s)
                for response in list(responses):
                    self.assertEqual(response.get("status"), "Error")
                    self.assertEqual(response.get("hash"), s.get("hash"))

    def test_Parser_server_init_and_monitor(self):
        """
        An end-to-end test of the gRPC server that sends an INIT request to the server,
        and the monitors it with the MONITOR task.
        """
        cls = initialize_parser()(
            self.producer, self.ser_schema, self.schema_client, self.logger.logger
        )
        record_id = str(uuid.uuid4())
        s = {"record_id": record_id, "status": "Error", "task": "ARXIV"}
        db.write_job_status(cls, s)

        s = {"record_id": record_id, "persistence": False, "task": "REPARSE"}
        with base.base_utils.mock_multiple_targets(
            {
                "update_job_status": patch.object(db, "update_job_status", return_value=True),
            }
        ):
            with grpc.insecure_channel(f"localhost:{self.port}") as channel:
                stub = parser_grpc.ParserInitStub(channel, self.avroserialhelper)
                responses = stub.initParser(s)
                output_hash = None
                for response in list(responses):
                    output_hash = response.get("record_id")
                    self.assertEqual(response.get("status"), "Pending")
                    self.assertNotEqual(response.get("record_id"), None)

                    s = {
                        "task": "MONITOR",
                        "record_id": output_hash,
                        "persistence": False,
                    }

            # Test update_job_status as well to mimic the Pipeline updating the status.
            db.update_job_status(cls, output_hash, status="Processing")

            with grpc.insecure_channel(f"localhost:{self.port}") as channel:
                stub = parser_grpc.ParserMonitorStub(channel, self.avroserialhelper)
                responses = stub.monitorParser(s)
                print(list(responses))
                for response in list(responses):
                    self.assertEqual(response.get("status"), "Processing")
                    self.assertEqual(response.get("record_id"), s.get("record_id"))

    def test_Parser_server_view(self):
        """
        A test of the MONITOR method for the gRPC server
        input:
            s: AVRO message: ParserInputSchema
        """
        s = {"task": "VIEW", "record_id": str(uuid.uuid4())}
        with open("SciXParser/tests/stubdata/arxiv_parsed_data.json", "r") as f:
            parsed_record = json.load(f)
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "get_parser_record": patch.object(
                        db,
                        "get_parser_record",
                        return_value=base.mock_reparse_db_entry(
                            str(s.get("record_id")),
                            "/{}".format(s.get("record_id")),
                            parsed_record,
                        ),
                    ),
                }
            ):
                stub = parser_grpc.ParserViewStub(channel, self.monclientavroserialhelper)
                responses = stub.viewParser(s)
                for response in list(responses):
                    self.assertEqual(response.get("record_id"), s.get("record_id"))
