import logging
import time
from collections import defaultdict

import requests as requests_lib
from hubstorage import HubstorageClient

from hcf_backend.slot import HCFSlot


LOG = logging.getLogger(__name__)


class HCFManager(object):

    def __init__(self, auth, project_id, frontier, batch_size=100, endpoint=None):
        self._hs_client = HubstorageClient(auth=auth, endpoint=endpoint)
        self._hcf = self._hs_client.get_project(project_id).frontier
        self._frontier = frontier
        self._links_count = defaultdict(int)
        self._links_to_flush_count = defaultdict(int)
        self._batch_size = batch_size
        self._hcf_slots = {}
        self._hcf_retries = 10

    def create_slot_if_needed(self, frontier, slot):
        key = (frontier, slot)
        if key not in self._hcf_slots:
            self._hcf_slots[key] = HCFSlot(self._hcf, frontier, slot,
                    batch_size=self._batch_size)
        return self._hcf_slots[key]

    def add_request(self, slot, request):
        hcf_slot = self.create_slot_if_needed(self._frontier, slot)
        hcf_slot.write_one(request)

    def flush(self, slot):
        hcf_slot = self.create_slot_if_needed(self._frontier, slot)
        hcf_slot.flush()

    def read(self, slot, mincount=None):
        hcf_slot = self.create_slot_if_needed(self._frontier, slot)
        for retry_times in range(1, self._hcf_retries + 1):
            try:
                return hcf_slot.read(count=mincount)
            except requests_lib.exceptions.ReadTimeout:
                LOG.error('Could not read from {0}/{1} try {2}/{3}'\
                        .format(self._frontier, slot, retry_times, self._hcf_retries))
            except requests_lib.exceptions.ConnectionError:
                LOG.error('Connection error while reading from {0}/{1} try {2}/{3}'\
                        .format(self._frontier, slot, retry_times, self._hcf_retries))
            except requests_lib.exceptions.RequestException:
                LOG.error('Error while reading from {0}/{1} try {2}/{3}'\
                        .format(self._frontier, slot, retry_times, self._hcf_retries))
            time.sleep(60 * retry_times)
        return []

    def delete(self, slot, ids):
        hcf_slot = self.create_slot_if_needed(self._frontier, slot)
        for retry_times in range(1, self._hcf_retries + 1):
            try:
                hcf_slot.delete(ids)
                break
            except requests_lib.exceptions.ReadTimeout:
                LOG.error('Could not delete ids from {0}/{1} try {2}/{3}'\
                        .format(self._frontier, slot, retry_times, self._hcf_retries))
            except requests_lib.exceptions.ConnectionError:
                LOG.error('Connection error while deleting ids from {0}/{1} try {2}/{3}'\
                        .format(self._frontier, slot, retry_times, self._hcf_retries))
            except requests_lib.exceptions.RequestException:
                LOG.error('Error deleting ids from {0}/{1} try {2}/{3}'\
                        .format(self._frontier, slot, retry_times, self._hcf_retries))
            time.sleep(60 * retry_times)

    def delete_slot(self, slot):
        hcf_slot = self.create_slot_if_needed(self._frontier, slot)
        hcf_slot.truncate()

    def close(self):
        self._hcf.close()
        self._hs_client.close()
