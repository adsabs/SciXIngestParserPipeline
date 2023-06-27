"""The Python AsyncIO implementation of the GRPC parser.ParserInit client."""

import argparse
import asyncio
import logging
import sys

import grpc
from confluent_kafka.schema_registry import SchemaRegistryClient
from SciXPipelineUtils.avro_serializer import AvroSerialHelper
from SciXPipelineUtils.utils import get_schema

import SciXParser.API.grpc_modules.parser_grpc as parser_grpc


class Logging:
    def __init__(self, logger):
        self.logger = logger


def input_parser(cli_args):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help="commands", dest="action")

    process_parser = subparsers.add_parser("REPARSE", help="Initialize a job with given inputs")
    process_parser.add_argument(
        "--persistence",
        action="store_true",
        dest="persistence",
        default=False,
        help="Specify whether server keeps channel open to client during processing.",
    )
    process_parser.add_argument(
        "--uuid",
        action="store",
        dest="uuid",
        type=str,
        help="The UUID of the record to be reparsed.",
    )

    process_parser = subparsers.add_parser("VIEW", help="Initialize a job with given inputs")
    process_parser.add_argument(
        "--persistence",
        action="store_true",
        dest="persistence",
        default=False,
        help="Specify whether server keeps channel open to client during processing.",
    )
    process_parser.add_argument(
        "--uuid",
        action="store",
        dest="uuid",
        type=str,
        help="The UUID of the record to be reparsed.",
    )

    process_parser = subparsers.add_parser("MONITOR", help="Initialize a job with given inputs")
    process_parser.add_argument(
        "--uuid", action="store", dest="uuid", type=str, help="Job ID string to query."
    )
    process_parser.add_argument(
        "--persistence",
        action="store_true",
        dest="persistence",
        default=False,
        help="Specify whether server keeps channel open to client during processing.",
    )
    args = parser.parse_args(cli_args)
    return args


def output_message(args):
    s = {}
    s["persistence"] = args.persistence
    s["task"] = args.action
    s["record_id"] = args.uuid
    return s


async def run() -> None:
    schema_client = SchemaRegistryClient({"url": "http://localhost:8081"})

    logger = Logging(logging)
    schema = get_schema(logger, schema_client, "ParserInputSchema")

    avroserialhelper = AvroSerialHelper(schema)

    args = input_parser(sys.argv[1:])
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        s = output_message(args)
        if s["task"] == "VIEW":
            try:
                stub = parser_grpc.ParserViewStub(channel, avroserialhelper)
                async for response in stub.viewParser(s):
                    print(response)

            except grpc.aio._call.AioRpcError as e:
                code = e.code()
                print(
                    "gRPC server connection failed with status {}: {}".format(
                        code.name, code.value
                    )
                )

        elif s["task"] == "MONITOR":
            try:
                stub = parser_grpc.ParserMonitorStub(channel, avroserialhelper)
                async for response in stub.monitorParser(s):
                    print(response)

            except grpc.aio._call.AioRpcError as e:
                code = e.code()
                print(
                    "gRPC server connection failed with status {}: {}".format(
                        code.name, code.value
                    )
                )

        elif s["task"] == "REPARSE":
            try:
                stub = parser_grpc.ParserInitStub(channel, avroserialhelper)
                async for response in stub.initParser(s):
                    print(response)

            except grpc.aio._call.AioRpcError as e:
                code = e.code()
                print(
                    "gRPC server connection failed with status {}: {}".format(
                        code.name, code.value
                    )
                )


if __name__ == "__main__":
    logging.basicConfig()
    asyncio.run(run())
