import logging
import time
from collections import defaultdict
from typing import Optional, Dict

import requests as requests_lib
import scrapinghub
from scrapinghub import ScrapinghubClient
from shub_workflow.utils import resolve_project_id

from hcf_backend.utils import get_apikey


LOG = logging.getLogger(__name__)


class HCFManager(object):
    def __init__(
        self,
        frontier: str,
        project_id: Optional[int] = None,
        auth: Optional[str] = None,
        batch_size: int = 0,
    ):
        if auth is None:
            auth = get_apikey()
        self._client = ScrapinghubClient(auth=auth)
        project_id = project_id or resolve_project_id()
        self._hcf = self._client.get_project(project_id).frontiers
        self._frontier = self._hcf.get(frontier)
        self._links_count: Dict[str, int] = defaultdict(int)
        self._links_to_flush_count: Dict[str, int] = defaultdict(int)
        self._batch_size = batch_size
        self._hcf_retries = 10

    def new_links_count(self):
        return self._hcf.newcount

    def add_request(self, slot: str, request):
        self._frontier.get(slot).q.add([request])
        self._links_count[slot] += 1
        self._links_to_flush_count[slot] += 1
        if self._batch_size and self._links_to_flush_count[slot] >= self._batch_size:
            return self.flush(slot)
        return 0

    def flush(self, slot=None):
        n_links_to_flush = self.get_number_of_links_to_flush(slot)
        if n_links_to_flush:
            if slot is None:
                self._hcf.flush()
                for slot in self._links_to_flush_count.keys():
                    self._links_to_flush_count[slot] = 0
                LOG.info("Flushed %d link(s).", n_links_to_flush)
            else:
                slot_obj = self._frontier.get(slot)
                slot_obj.flush()
                self._links_to_flush_count[slot] = 0

        return n_links_to_flush

    def read(self, slot, mincount=None):
        slot_obj = self._frontier.get(slot)
        for i in range(self._hcf_retries):
            try:
                return slot_obj.q.iter(mincount=mincount)
            except requests_lib.exceptions.ReadTimeout:
                LOG.error(
                    "Could not read from {0}/{1} try {2}/{3}".format(
                        self._frontier.key, slot, i + 1, self._hcf_retries
                    )
                )
            except (
                requests_lib.exceptions.ConnectionError,
                scrapinghub.client.exceptions.ServerError,
            ):
                LOG.error(
                    "Connection error while reading from {0}/{1} try {2}/{3}".format(
                        self._frontier.key, slot, i + 1, self._hcf_retries
                    )
                )
            except requests_lib.exceptions.RequestException:
                LOG.error(
                    "Error while reading from {0}/{1} try {2}/{3}".format(
                        self._frontier.key, slot, i + 1, self._hcf_retries
                    )
                )
            time.sleep(60 * (i + 1))
        return []

    def delete(self, slot, ids):
        slot_obj = self._frontier.get(slot)

        for i in range(self._hcf_retries):
            try:
                slot_obj.q.delete(ids)
                break
            except requests_lib.exceptions.ReadTimeout:
                LOG.error(
                    "Could not delete ids from {0}/{1} try {2}/{3}".format(
                        self._frontier.key, slot, i + 1, self._hcf_retries
                    )
                )
            except (
                requests_lib.exceptions.ConnectionError,
                scrapinghub.client.exceptions.ServerError,
            ):
                LOG.error(
                    "Connection error while deleting ids from {0}/{1} try {2}/{3}".format(
                        self._frontier.key, slot, i + 1, self._hcf_retries
                    )
                )
            except requests_lib.exceptions.RequestException:
                LOG.error(
                    "Error deleting ids from {0}/{1} try {2}/{3}".format(
                        self._frontier.key, slot, i + 1, self._hcf_retries
                    )
                )
            time.sleep(60 * (i + 1))

    def delete_slot(self, slot):
        slot_obj = self._frontier.get(slot)
        slot_obj.delete()

    def close(self):
        self._hcf.close()
        self._client.close()

    def get_number_of_links(self, slot=None):
        if slot is None:
            return sum(self._links_count.values())
        else:
            return self._links_count[slot]

    def get_number_of_links_to_flush(self, slot=None):
        if slot is None:
            return sum(self._links_to_flush_count.values())
        else:
            return self._links_to_flush_count[slot]
