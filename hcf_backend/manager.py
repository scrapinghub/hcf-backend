import logging
from time import time, sleep
from collections import defaultdict

import requests as requests_lib
from hubstorage import HubstorageClient
from frontera.contrib.backends.memory import MemoryStates
from json import loads


LOG = logging.getLogger(__name__)


class HCFStates(MemoryStates):

    def __init__(self, auth, project_id, frontier, cache_size_limit):
        super(HCFStates, self).__init__(cache_size_limit)
        self._hs_client = HubstorageClient(auth=auth)
        self.projectid = project_id
        project = self._hs_client.get_project(self.projectid)
        self._collections = project.collections
        self.logger = logging.getLogger("HCFStates")

    def frontier_start(self):
        self._store = self._collections.new_store("states")

    def fetch(self, fingerprints):
        to_fetch = [f for f in fingerprints if f not in self._cache]
        self.logger.debug("cache size %s" % len(self._cache))
        self.logger.debug("to fetch %d from %d" % (len(to_fetch), len(fingerprints)))

        prepared_keys = ["key=%s" % f for f in to_fetch]
        params = {'method':'GET',
                  'url':'https://storage.scrapinghub.com/collections/%d/s/%s' % (self.projectid, "states"),
                  'params':str('&').join(prepared_keys),
                  'auth':self._hs_client.auth}
        response = self._hs_client.session.request(**params)
        assert response.status_code == 200
        fprint = iter(to_fetch)
        # TODO: fix the situations when key is absent
        for line in response.content.split('\n'):
            o = loads(line)
            self._cache[fprint.next()] = o['value']

    def flush(self, force_clear=False):
        buffer = []
        try:
            for fprint, state_val in self._cache.iteritems():
                buffer.append({'_key': fprint, 'value':state_val})
                if len(buffer) > 64:
                    self.logger.debug("Flushing %d items." % len(buffer))
                    self._store.set(buffer)
                    buffer = []
        finally:
            self._store.set(buffer)
        self.logger.debug("State cache has been flushed.")
        super(HCFStates, self).flush(force_clear)


class HubstorageCrawlFrontier(object):

    def __init__(self, auth, project_id, frontier, batch_size=0, flush_interval=30):
        self._hs_client = HubstorageClient(auth=auth)
        self._hcf = self._hs_client.get_project(project_id).frontier
        self._hcf.batch_size = batch_size
        self._hcf.batch_interval = flush_interval
        self._frontier = frontier
        self._links_count = defaultdict(int)
        self._links_to_flush_count = defaultdict(int)
        self._hcf_retries = 10

    def add_request(self, slot, request):
        self._hcf.add(self._frontier, slot, [request])
        self._links_count[slot] += 1
        self._links_to_flush_count[slot] += 1
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
            sleep(60 * (i + 1))
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
            sleep(60 * (i + 1))

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
