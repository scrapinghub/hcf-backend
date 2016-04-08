# -*- coding: utf-8 -*-
from hcf_backend.backend import HCFQueue
from frontera.core.models import Request
from time import sleep
import logging


def test_queue():
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("HCFQueue")
    queue = HCFQueue(logger, "5f755afefab74ad9843c8d9a4633876b", 49484, "test", 10000, 1, 1, "", True)

    queue.frontier_start()

    r = Request(url="http://scrapinghub.com", meta={"fingerprint": "abcdef01234567890"})
    queue.schedule([("", 0.9, r, True)])
    sleep(4)
    result = queue.get_next_requests(256, 0)
    assert result[0].url == r.url
    queue.frontier_stop()
