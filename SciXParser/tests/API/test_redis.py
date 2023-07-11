import json
import logging
from unittest import TestCase

import redis

import API.parser_server as hs
from SciXParser.parser.db import write_status_redis


class TestRedisReadWrite(TestCase):
    def test_redis_read_write(self):
        listener = hs.Listener()
        listener.subscribe()
        job_id = "1234234215"
        status = "Success"
        logger = hs.Logging(logging)
        redis_status = json.dumps({"job_id": job_id, "status": status})
        redis_instance = redis.StrictRedis(
            "localhost",
            6379,
            decode_responses=True,
        )
        write_status_redis(redis_instance, redis_status)
        status = next(listener.get_status_redis(job_id, logger.logger))
        self.assertEqual(status, status)
