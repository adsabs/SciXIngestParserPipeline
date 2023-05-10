"""The Python AsyncIO implementation of the GRPC template.TemplateInit client."""

import argparse
import asyncio
import json
import logging
import sys

import grpc
from confluent_kafka.schema_registry import SchemaRegistryClient

import SciXTEMPLATE.API.grpc_modules.template_grpc as template_grpc
from SciXTEMPLATE.API.avro_serializer import AvroSerialHelper
from SciXTEMPLATE.TEMPLATE.utils import get_schema


class Logging:
    def __init__(self, logger):
        self.logger = logger


def input_parser(cli_args):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help="commands", dest="action")
    process_parser = subparsers.add_parser(
        "TEMPLATE_INIT", help="Initialize a job with given inputs"
    )
    process_parser.add_argument(
        "--task_args",
        action="store",
        dest="job_args",
        type=str,
        help="JSON dump containing arguments for Template Processes",
    )
    process_parser.add_argument(
        "--persistence",
        action="store_true",
        dest="persistence",
        default=False,
        help="Specify whether server keeps channel open to client during processing.",
    )
    process_parser.add_argument(
        "--task",
        action="store",
        dest="task",
        type=str,
        help="Specify whether server keeps channel open to client during processing.",
    )

    process_parser = subparsers.add_parser(
        "TEMPLATE_MONITOR", help="Initialize a job with given inputs"
    )
    process_parser.add_argument(
        "--job_id", action="store", dest="job_id", type=str, help="Job ID string to query."
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
    if args.action == "TEMPLATE_INIT":
        if args.job_args:
            task_args = json.loads(args.job_args)
            s["task_args"] = task_args
            if task_args.get("ingest"):
                s["task_args"]["ingest"] = bool(task_args["ingest"])
        s["task_args"]["persistence"] = args.persistence
        s["task"] = args.task
    elif args.action == "TEMPLATE_MONITOR":
        s["task"] = "MONITOR"
        s["task_args"] = {"persistence": args.persistence}
        s["hash"] = args.job_id
    return s


async def run() -> None:
    schema_client = SchemaRegistryClient({"url": "http://localhost:8081"})

    logger = Logging(logging)
    schema = get_schema(logger, schema_client, "TemplateInputSchema")

    avroserialhelper = AvroSerialHelper(schema)

    args = input_parser(sys.argv[1:])
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        s = output_message(args)
        if s["task"] == "MONITOR":
            try:
                stub = template_grpc.TemplateMonitorStub(channel, avroserialhelper)
                async for response in stub.monitorTemplate(s):
                    print(response)

            except grpc.aio._call.AioRpcError as e:
                code = e.code()
                print(
                    "gRPC server connection failed with status {}: {}".format(
                        code.name, code.value
                    )
                )

        else:
            try:
                stub = template_grpc.TemplateInitStub(channel, avroserialhelper)
                async for response in stub.initTemplate(s):
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
