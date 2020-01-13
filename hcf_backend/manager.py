import logging
import time
from collections import defaultdict

import requests as requests_lib
from scrapinghub import ScrapinghubClient


LOG = logging.getLogger(__name__)


class HCFManager(object):

    def __init__(self, auth, project_id, frontier, batch_size=0):
        self._client = ScrapinghubClient(auth=auth)
        self._hcf = self._client.get_project(project_id).frontiers
        self._frontier = self._hcf.get(frontier)
        self._links_count = defaultdict(int)
        self._links_to_flush_count = defaultdict(int)
        self._batch_size = batch_size
        self._hcf_retries = 10

    def add_request(self, slot, request):
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
                LOG.info('Flushed %d link(s).', n_links_to_flush)
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
                LOG.error("Could not read from {0}/{1} try {2}/{3}".format(self._frontier.key, slot, i+1,
                                                                      self._hcf_retries))
            except requests_lib.exceptions.ConnectionError:
                LOG.error("Connection error while reading from {0}/{1} try {2}/{3}".format(self._frontier.key, slot, i+1,
                                                                      self._hcf_retries))
            except requests_lib.exceptions.RequestException:
                LOG.error("Error while reading from {0}/{1} try {2}/{3}".format(self._frontier.key, slot, i+1,
                                                                      self._hcf_retries))
            time.sleep(60 * (i + 1))
        return []

    def delete(self, slot, ids):
        slot_obj = self._frontier.get(slot)

        for i in range(self._hcf_retries):
            try:
                slot_obj.q.delete(ids)
                break
            except requests_lib.exceptions.ReadTimeout:
                LOG.error("Could not delete ids from {0}/{1} try {2}/{3}".format(
                    self._frontier.key, slot, i+1, self._hcf_retries))
            except requests_lib.exceptions.ConnectionError:
                LOG.error("Connection error while deleting ids from {0}/{1} try {2}/{3}".format(
                    self._frontier.key, slot, i+1, self._hcf_retries))
            except requests_lib.exceptions.RequestException:
                LOG.error("Error deleting ids from {0}/{1} try {2}/{3}".format(
                    self._frontier.key, slot, i+1, self._hcf_retries))
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
