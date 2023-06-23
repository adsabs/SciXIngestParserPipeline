import json
from unittest import TestCase

from SciXPipelineUtils import avro_serializer

from tests.parser.base import mock_avro_test


class TestAvroOutputSerializer(TestCase):
    def test_avro_serialization(self):
        with open("SciXParser/tests/stubdata/AVRO_schemas/ParserOutputSchema.avsc") as f:
            schema_json = json.load(f)

        msg = mock_avro_test().value()
        serializer = avro_serializer.AvroSerialHelper(json.dumps(schema_json))
        bitstream = serializer.avro_serializer(msg)
        self.assertEqual(bitstream, mock_avro_test().bitstream())

    def test_avro_deserialization(self):
        with open("SciXParser/tests/stubdata/AVRO_schemas/ParserOutputSchema.avsc") as f:
            schema_json = json.load(f)
        serializer = avro_serializer.AvroSerialHelper(json.dumps(schema_json))
        bitstream = mock_avro_test().bitstream()
        msg = serializer.avro_deserializer(bitstream)
        self.assertEqual(msg, mock_avro_test().deserialized_value())
