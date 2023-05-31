import logging
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
from API.parser_server import Listener, Logging, Parser
from parser import db
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
        self.VALUE_SCHEMA_NAME = "ParserInputSchema"
        self.value_schema = open(self.VALUE_SCHEMA_FILE).read()

        self.schema_client.register(self.VALUE_SCHEMA_NAME, Schema(self.value_schema, "AVRO"))
        self.schema = get_schema(self.logger, self.schema_client, self.VALUE_SCHEMA_NAME)
        self.avroserialhelper = AvroSerialHelper(self.schema, self.logger.logger)

        OUTPUT_VALUE_SCHEMA_FILE = "SciXParser/tests/stubdata/AVRO_schemas/ParserOutputSchema.avsc"
        OUTPUT_VALUE_SCHEMA_NAME = "ParserOutputSchema"
        output_value_schema = open(OUTPUT_VALUE_SCHEMA_FILE).read()

        self.schema_client.register(OUTPUT_VALUE_SCHEMA_NAME, Schema(output_value_schema, "AVRO"))
        self.producer = AvroProducer({}, schema_registry=MockSchemaRegistryClient())

        parser_grpc.add_ParserInitServicer_to_server(
            Parser(self.producer, self.schema, self.schema_client, self.logger.logger),
            self.server,
            self.avroserialhelper,
        )

        parser_grpc.add_ParserMonitorServicer_to_server(
            Parser(self.producer, self.schema, self.schema_client, self.logger.logger),
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
        s = {
            "task_args": {
                "ingest": True,
                "ingest_type": "metadata",
                "daterange": "2023-04-26",
                "persistence": False,
            },
            "task": "SYMBOL1",
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            stub = parser_grpc.ParserInitStub(channel, self.avroserialhelper)
            responses = stub.initParser(s)
            for response in list(responses):
                self.assertEqual(response.get("status"), "Pending")
                self.assertNotEqual(response.get("hash"), None)

    def test_Parser_server_init_persistence(self):
        """
        A test of the INIT method for the gRPC server with persistence
        input:
            s: AVRO message: ParserInputSchema
        """
        s = {
            "task_args": {
                "ingest": True,
                "ingest_type": "metadata",
                "daterange": "2023-04-26",
                "persistence": True,
            },
            "task": "SYMBOL1",
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "write_job_status": patch.object(db, "write_job_status", return_value=True),
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry("Processing")
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
                    self.assertNotEqual(response.get("hash"), None)
                    final_response.append(response.get("status"))
                self.assertEqual(final_response, ["Pending", "Processing", "Success"])

    def test_Parser_server_init_persistence_error_redis(self):
        """
        A test of the INIT method for the gRPC server with persistence
        where an error is returned by the redis server.
        input:
            s: AVRO message: ParserInputSchema
        """
        s = {
            "task_args": {
                "ingest": True,
                "ingest_type": "metadata",
                "daterange": "2023-04-26",
                "persistence": True,
            },
            "task": "SYMBOL1",
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "write_job_status": patch.object(db, "write_job_status", return_value=True),
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry("Processing")
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
                    self.assertNotEqual(response.get("hash"), None)
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
            "task_args": {
                "ingest": True,
                "ingest_type": "metadata",
                "daterange": "2023-04-26",
                "persistence": True,
            },
            "task": "SYMBOL1",
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "write_job_status": patch.object(db, "write_job_status", return_value=True),
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry("Error")
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
                    self.assertNotEqual(response.get("hash"), None)
                    final_response.append(response.get("status"))
                self.assertEqual(final_response, ["Pending", "Error"])

    def test_Parser_server_monitor(self):
        """
        A test of the MONITOR method for the gRPC server
        input:
            s: AVRO message: ParserInputSchema
        """
        s = {
            "task": "MONITOR",
            "hash": "c98b5b0f5e4dce3197a4a9a26d124d036f293a9a90a18361f475e4f08c19f2da",
            "task_args": {"persistence": False},
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry()
                    )
                }
            ):
                stub = parser_grpc.ParserMonitorStub(channel, self.avroserialhelper)
                responses = stub.monitorParser(s)
                for response in list(responses):
                    self.assertEqual(response.get("status"), "Success")
                    self.assertEqual(response.get("hash"), s.get("hash"))

    def test_Parser_server_monitor_persistent_success(self):
        """
        A test of the MONITOR method for the gRPC server with persistence
        input:
            s: AVRO message: ParserInputSchema
        """
        s = {
            "task": "MONITOR",
            "hash": "c98b5b0f5e4dce3197a4a9a26d124d036f293a9a90a18361f475e4f08c19f2da",
            "task_args": {"persistence": True},
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "write_job_status": patch.object(db, "write_job_status", return_value=True),
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry("Success")
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
        s = {
            "task": "MONITOR",
            "hash": "c98b5b0f5e4dce3197a4a9a26d124d036f293a9a90a18361f475e4f08c19f2da",
            "task_args": {"persistence": True},
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "write_job_status": patch.object(db, "write_job_status", return_value=True),
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry("Error")
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
        s = {
            "task": "MONITOR",
            "hash": "c98b5b0f5e4dce3197a4a9a26d124d036f293a9a90a18361f475e4f08c19f2da",
            "task_args": {"persistence": True},
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "write_job_status": patch.object(db, "write_job_status", return_value=True),
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry("Processing")
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
        s = {
            "task": "MONITOR",
            "hash": None,
            "task_args": {"persistence": True},
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            with base.base_utils.mock_multiple_targets(
                {
                    "write_job_status": patch.object(db, "write_job_status", return_value=True),
                    "get_job_status_by_job_hash": patch.object(
                        db, "get_job_status_by_job_hash", return_value=fake_db_entry("Error")
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
        cls = Parser(self.producer, self.schema, self.schema_client, self.logger.logger)
        s = {
            "task_args": {
                "ingest": True,
                "ingest_type": "metadata",
                "daterange": "2023-04-26",
                "persistence": False,
            },
            "task": "SYMBOL1",
        }
        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            stub = parser_grpc.ParserInitStub(channel, self.avroserialhelper)
            responses = stub.initParser(s)
            output_hash = None
            for response in list(responses):
                output_hash = response.get("hash")
                self.assertEqual(response.get("status"), "Pending")
                self.assertNotEqual(response.get("hash"), None)

                s = {
                    "task": "MONITOR",
                    "hash": output_hash,
                    "task_args": {"persistence": False},
                }

        # Test update_job_status as well to mimic the Pipeline updating the status.
        db.update_job_status(cls, output_hash, status="Processing")

        with grpc.insecure_channel(f"localhost:{self.port}") as channel:
            stub = parser_grpc.ParserMonitorStub(channel, self.avroserialhelper)
            responses = stub.monitorParser(s)
            for response in list(responses):
                self.assertEqual(response.get("status"), "Processing")
                self.assertEqual(response.get("hash"), s.get("hash"))
