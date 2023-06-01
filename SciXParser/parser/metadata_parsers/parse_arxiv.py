from datetime import datetime

from adsingestp.parsers.arxiv import ArxivParser

from SciXParser.parser import db


def parse_store_arxiv_record(app, job_request, reparse=False):

    record_id = job_request.get("record_id")
    s3_key = job_request.get("s3_key")
    task = job_request.get("task")
    metadata = job_request.get("record_xml")
    date = datetime.now()

    parsed_record = ArxivParser.parse(metadata)

    try:
        if reparse:
            record_status = db.update_parser_record_metadata(record_id, date, parsed_record)
        else:
            record_status = db.write_parser_record(
                app, record_id, date, s3_key, parsed_record, task
            )
    except Exception as e:
        status = "ERROR"
        db.update_job_status(record_id, status)
        app.logger.error(
            "Failed to write record {} to database with error: {}.".format(record_id, e)
        )
        return status

    if record_status:
        try:
            app.producer.produce()
            status = "SUCCESS"
        except Exception:
            status = "ERROR"
        db.update_job_status(record_id, status)
        return status
