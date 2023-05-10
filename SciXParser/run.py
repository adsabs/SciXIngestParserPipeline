import argparse
import asyncio
import os
from parser import parser

from API import parser_server

if __name__ == "__main__":
    Parser = argparse.ArgumentParser()
    subparsers = Parser.add_subparsers(help="commands", dest="action")
    subparsers.add_parser("PARSER_API", help="Initialize Parser gRPC API")
    subparsers.add_parser("PARSER_APP", help="Initialize Parser Working Unit")
    args = Parser.parse_args()

    if args.action == "PARSER_APP":
        proj_home = os.path.realpath("/app/SciXParser/")
        parser.init_pipeline(proj_home)

    elif args.action == "PARSER_API":
        asyncio.run(parser_server.serve())
