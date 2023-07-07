import json
from datetime import datetime

from SciXPipelineUtils import utils

from parser.metadata_parsers import parse_arxiv
from SciXParser.parser import db


def reparse_handler(app, job_request, producer):
    """
    Collects S3 information and prepares a compatible request for the task_selector
    """

    metadata_uuid = job_request.get("record_id")
    record_entry = db.get_parser_record(app, metadata_uuid)

    # If resend, only resend the data from the DB, do not initiate a parsing task.
    if job_request.get("resend") and record_entry:
        producer_message = record_entry.parsed_record
        producer_message["task"] = record_entry.source
        producer_message["record_id"] = metadata_uuid
        parser_output_schema = utils.get_schema(
            app, app.schema_client, app.config.get("PARSER_OUTPUT_SCHEMA")
        )

        try:
            producer.produce(
                topic=app.config.get("PARSER_OUTPUT_TOPIC"),
                value=producer_message,
                value_schema=parser_output_schema,
            )
            status = "Success"

        except Exception:
            app.logger.exception(
                "Failed to produce {} to Kafka topic: {}".format(
                    metadata_uuid, app.config.get("PARSER_OUTPUT_TOPIC")
                )
            )
            status = "Error"

        return status

    s3_path = record_entry.s3_key
    date = datetime.now()

    app.logger.info("Attempting to collect raw metadata from {} S3".format("AWS"))
    metadata = db.collect_metadata_from_secondary_s3(app, s3_path, job_request, metadata_uuid)

    new_job_request = {
        "record_id": metadata_uuid,
        "record_xml": metadata,
        "s3_path": s3_path,
        "task": record_entry.source,
        "datetime": date,
    }

    return parse_task_selector(app, new_job_request, producer, reparse=True)


def parse_task_selector(app, job_request, producer, reparse=False):
    """
    Identifies the correct task and calls the appropriate parser
    """
    task = job_request.get("task")
    if task == "ARXIV":
        print("Record is arxiv")
        parse_arxiv.parse_store_arxiv_record(app, job_request, producer, reparse=reparse)

    else:
        app.logger.error("{} is not a valid data source. Stopping.".format(task))
        status = "Error"
        db.write_status_redis(
            app.redis, json.dumps({"job_id": str(job_request.get("record_id")), "status": status})
        )
        db.update_job_status(app, job_request["record_id"], status)
