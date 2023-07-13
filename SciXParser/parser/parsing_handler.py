import json
from datetime import datetime

from SciXPipelineUtils import utils

from SciXParser.parser import db
from SciXParser.parser.metadata_parsers import parse_arxiv


def reparse_handler(app, job_request, producer):
    """
    Collects S3 information and prepares a compatible request for the task_selector
    """

    metadata_uuid = job_request.get("record_id")
    with app.session_scope() as session:
        record_entry = db.get_parser_record(session, metadata_uuid)
        try:
            record_source = record_entry.source.name
            producer_message = {}
            if record_entry.parsed_data:
                producer_message["task"] = record_source
                producer_message["record_id"] = str(metadata_uuid)
                producer_message["parsed_record"] = record_entry.parsed_data
            s3_path = record_entry.s3_key
        except AttributeError:
            app.logger.exception(
                f"Failed to collect record {metadata_uuid} from db. Are we sure it exists?"
            )
            status = "Error"
            producer_message = None
            db.update_job_status(app, job_request["record_id"], status)

    # If resend, only resend the data from the DB, do not initiate a parsing task.
    if job_request.get("resend") and producer_message.get("parsed_record"):
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

        except ValueError:
            app.logger.exception(
                "Failed to produce {} to Kafka topic: {}".format(
                    metadata_uuid, app.config.get("PARSER_OUTPUT_TOPIC")
                )
            )
            status = "Error"

        db.update_job_status(app, job_request["record_id"], status)

        return status

    elif job_request.get("resend"):
        app.logger.error(
            "Failed to produce {} to Kafka topic: {} because there is no parsed record to resend.".format(
                metadata_uuid, app.config.get("PARSER_OUTPUT_TOPIC")
            )
        )
        status = "Error"

        db.update_job_status(app, job_request["record_id"], status)

        return status

    date = datetime.now()

    app.logger.info("Attempting to collect raw metadata from {} S3".format("AWS"))
    metadata = db.collect_metadata_from_secondary_s3(app, s3_path, job_request, metadata_uuid)

    new_job_request = {
        "record_id": metadata_uuid,
        "record_xml": metadata,
        "s3_path": s3_path,
        "task": record_source,
        "datetime": date,
        "force": job_request.get("force"),
    }

    return parse_task_selector(app, new_job_request, producer, reparse=True)


def parse_task_selector(app, job_request, producer, reparse=False):
    """
    Identifies the correct task and calls the appropriate parser
    """
    task = job_request.get("task")
    if task == "ARXIV":
        parse_arxiv.parse_store_arxiv_record(app, job_request, producer, reparse=reparse)

    else:
        app.logger.error("{} is not a valid data source. Stopping.".format(task))
        status = "Error"
        db.write_status_redis(
            app.redis, json.dumps({"job_id": str(job_request.get("record_id")), "status": status})
        )
        db.update_job_status(app, job_request["record_id"], status)
