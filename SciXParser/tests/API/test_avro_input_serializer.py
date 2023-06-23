import json
from unittest import TestCase

from SciXPipelineUtils import avro_serializer

from tests.API.base import mock_gRPC_avro_msg


class TestAvroInputSerializer(TestCase):
    def test_avro_serialization(self):
        with open("SciXParser/tests/stubdata/AVRO_schemas/ParserInputSchema.avsc") as f:
            schema_json = json.load(f)
        msg = mock_gRPC_avro_msg().value()
        serializer = avro_serializer.AvroSerialHelper(json.dumps(schema_json))
        bitstream = serializer.avro_serializer(msg)
        self.assertEqual(bitstream, mock_gRPC_avro_msg().bitstream())

    def test_avro_deserialization(self):
        with open("SciXParser/tests/stubdata/AVRO_schemas/ParserInputSchema.avsc") as f:
            schema_json = json.load(f)
        serializer = avro_serializer.AvroSerialHelper(json.dumps(schema_json))
        bitstream = mock_gRPC_avro_msg().bitstream()
        msg = serializer.avro_deserializer(bitstream)
        self.assertEqual(msg, mock_gRPC_avro_msg().value())
