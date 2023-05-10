import argparse
import asyncio
import os

from TEMPLATE import template

from API import template_server

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help="commands", dest="action")
    subparsers.add_parser("TEMPLATE_API", help="Initialize TEMPLATE gRPC API")
    subparsers.add_parser("TEMPLATE_APP", help="Initialize TEMPLATE Working Unit")
    args = parser.parse_args()

    if args.action == "TEMPLATE_APP":
        proj_home = os.path.realpath("/app/SciXTEMPLATE/")
        template.init_pipeline(proj_home)

    elif args.action == "TEMPLATE_API":
        asyncio.run(template_server.serve())
