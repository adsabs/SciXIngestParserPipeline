import logging
from unittest import TestCase

import pytest
from confluent_kafka.schema_registry import Schema
from SciXPipelineUtils.utils import get_schema

from API.parser_client import Logging, input_parser, output_message
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
        input_args = ["MONITOR", "--uuid", "206f479f-bb1e-49ff-96df-491d66769abc"]
        args = input_parser(input_args)
        self.assertEqual(args.action, input_args[0])
        self.assertEqual(args.uuid, input_args[2])

        s = output_message(args)
        self.assertEqual(s["task"], "MONITOR")

        input_args = [
            "REPARSE",
            "--uuid",
            "206f479f-bb1e-49ff-96df-491d66769abc",
            "--persistence",
            "--force",
        ]
        args = input_parser(input_args)
        self.assertEqual(args.action, input_args[0])
        self.assertEqual(args.uuid, input_args[2])
        self.assertEqual(args.persistence, True)
        self.assertEqual(args.force, True)

        s = output_message(args)
        self.assertEqual(s["task"], "REPARSE")

        input_args = [
            "REPARSE",
            "--uuid",
            "206f479f-bb1e-49ff-96df-491d66769abc",
            "--persistence",
            "--resend-only",
        ]
        args = input_parser(input_args)
        self.assertEqual(args.action, input_args[0])
        self.assertEqual(args.uuid, input_args[2])
        self.assertEqual(args.persistence, True)
        self.assertEqual(args.resend, True)
        self.assertEqual(args.force, False)
