import logging
import time
from collections import defaultdict

import requests as requests_lib
from scrapinghub.hubstorage import HubstorageClient


LOG = logging.getLogger(__name__)


class HCFManager(object):

    def __init__(self, auth, project_id, frontier, batch_size=0):
        self._hs_client = HubstorageClient(auth=auth)
        self._hcf = self._hs_client.get_project(project_id).frontier
        self._frontier = frontier
        self._links_count = defaultdict(int)
        self._links_to_flush_count = defaultdict(int)
        self._batch_size = batch_size
        self._hcf_retries = 10

    def add_request(self, slot, request):
        self._hcf.add(self._frontier, slot, [request])
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
            else:
                writer = self._hcf._get_writer(self._frontier, slot)
                writer.flush()
                self._links_to_flush_count[slot] = 0
        return n_links_to_flush

    def read(self, slot, mincount=None):
        for i in range(self._hcf_retries):
            try:
                return self._hcf.read(self._frontier, slot, mincount)
            except requests_lib.exceptions.ReadTimeout:
                LOG.error("Could not read from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                      self._hcf_retries))
            except requests_lib.exceptions.ConnectionError:
                LOG.error("Connection error while reading from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                      self._hcf_retries))
            except requests_lib.exceptions.RequestException:
                LOG.error("Error while reading from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                      self._hcf_retries))
            time.sleep(60 * (i + 1))
        return []

    def delete(self, slot, ids):
        for i in range(self._hcf_retries):
            try:
                self._hcf.delete(self._frontier, slot, ids)
                break
            except requests_lib.exceptions.ReadTimeout:
                LOG.error("Could not delete ids from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                            self._hcf_retries))
            except requests_lib.exceptions.ConnectionError:
                LOG.error("Connection error while deleting ids from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                            self._hcf_retries))
            except requests_lib.exceptions.RequestException:
                LOG.error("Error deleting ids from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                            self._hcf_retries))
            time.sleep(60 * (i + 1))

    def delete_slot(self, slot):
        self._hcf.delete_slot(self._frontier, slot)

    def close(self):
        self._hcf.close()
        self._hs_client.close()

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
