import argparse
import asyncio
import os
from multiprocessing import Process

from SciXPipelineUtils import utils

from API import parser_server
from parser import parser

if __name__ == "__main__":
    Parser = argparse.ArgumentParser()
    subparsers = Parser.add_subparsers(help="commands", dest="action")
    subparsers.add_parser("PARSER_API", help="Initialize Parser gRPC API")
    subparsers.add_parser("PARSER_APP", help="Initialize Parser Working Unit")
    args = Parser.parse_args()
    if args.action == "PARSER_APP":
        path = os.path.dirname(__file__)
        proj_home = os.path.realpath(path)
        config = utils.load_config(proj_home)

        proj_home = path
        consumer_topic_name = config.get("HARVESTER_OUTPUT_TOPIC")
        consumer_schema_name = config.get("HARVESTER_OUTPUT_SCHEMA")

        # Initialize parser that consumes from Input Topic
        # parser.init_pipeline(proj_home)
        # Initialize parser that consumes from Harvester Output Topic
        # parser.init_pipeline(proj_home, consumer_topic_name=config.get("HARVESTER_OUTPUT_TOPIC"), consumer_schema_name=config.get("HARVESTER_OUTPUT_SCHEMA"))
        Process(target=parser.init_pipeline, args=(proj_home, None, None)).start()
        Process(
            target=parser.init_pipeline,
            args=(proj_home, consumer_topic_name, consumer_schema_name),
        ).start()

    elif args.action == "PARSER_API":
        asyncio.run(parser_server.serve())
