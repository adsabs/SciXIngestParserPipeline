import logging
from unittest import TestCase

import pytest
from confluent_kafka.schema_registry import Schema

from API.parser_client import Logging, input_parser, output_message
from SciXParser.parser.utils import get_schema
from tests.common.mockschemaregistryclient import MockSchemaRegistryClient


class TestParserClient(TestCase):
    def test_get_schema(self):
        logger = Logging(logging)
        schema_client = MockSchemaRegistryClient()
        VALUE_SCHEMA_FILE = "SciXParser/tests/stubdata/AVRO_schemas/ParserInputSchema.avsc"
        VALUE_SCHEMA_NAME = "ParserInputSchema"
        value_schema = open(VALUE_SCHEMA_FILE).read()

        schema_client.register(VALUE_SCHEMA_NAME, Schema(value_schema, "AVRO"))
        schema = get_schema(logger, schema_client, VALUE_SCHEMA_NAME)
        self.assertEqual(value_schema, schema)

    def test_get_schema_failure(self):
        logger = Logging(logging)
        schema_client = MockSchemaRegistryClient()
        with pytest.raises(Exception):
            get_schema(logger, schema_client, "FakeSchema")

    def test_input_parser(self):
        input_args = [
            "PARSER_MONITOR",
            "--job_id",
            "'c98b5b0f5e4dce3197a4a9a26d124d036f293a9a90a18361f475e4f08c19f2da'",
        ]
        args = input_parser(input_args)
        self.assertEqual(args.action, input_args[0])
        self.assertEqual(args.job_id, input_args[2])

        s = output_message(args)
        self.assertEqual(s["task"], "MONITOR")

        input_args = [
            "PARSER_INIT",
            "--task",
            "SYMBOL1",
            "--task_args",
            '{"ingest": "True", "ingest_type": "metadata", "daterange":"2023-04-26"}',
            "--persistence",
        ]
        args = input_parser(input_args)
        self.assertEqual(args.action, input_args[0])
        self.assertEqual(args.task, input_args[2])
        self.assertEqual(args.job_args, input_args[4])
        self.assertEqual(args.persistence, True)

        s = output_message(args)
        self.assertEqual(s["task"], "SYMBOL1")
