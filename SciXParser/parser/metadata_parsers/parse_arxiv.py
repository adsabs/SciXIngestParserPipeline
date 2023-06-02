from datetime import datetime

from adsingestp.parsers.arxiv import ArxivParser

from parser import db


def parse_store_arxiv_record(app, job_request, reparse=False):

    record_id = job_request.get("record_id")
    s3_key = job_request.get("s3_path")
    task = job_request.get("task")
    metadata = job_request.get("record_xml")
    date = datetime.now()
    arxiv_parser = ArxivParser()

    try:
        parsed_record = arxiv_parser.parse(metadata)
        app.logger.debug("Parsed record is: {}".format(parsed_record))
        try:
            if reparse:
                app.logger.debug("{}".format(parsed_record))
                record_status = db.update_parser_record_metadata(
                    app, record_id, date, parsed_record
                )
            else:
                record_status = db.write_parser_record(
                    app, record_id, date, s3_key, parsed_record, task
                )
        except Exception:
            status = "Error"
            db.update_job_status(app, record_id, status)
            app.logger.exception("Failed to write record {}.".format(record_id))
            return status

        if record_status:
            try:
                # app.producer.produce()
                status = "Success"
            except Exception:
                status = "Error"

    except Exception:
        status = "Error"
        app.logger.exception("Failed to parse record metadata for record: {}".format(record_id))

    db.update_job_status(app, record_id, status)

    return status
