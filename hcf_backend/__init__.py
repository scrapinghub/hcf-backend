import logging
from collections import defaultdict
from datetime import datetime
from json import loads
from time import time, sleep

import requests as requests_lib
from frontera import Request
from frontera.contrib.backends import CommonBackend
from frontera.contrib.backends.memory import MemoryStates, MemoryMetadata
from frontera.contrib.backends.partitioners import FingerprintPartitioner
from frontera.core.components import Queue
from hubstorage import HubstorageClient


class HCFStates(MemoryStates):

    def __init__(self, auth, project_id, colname, cache_size_limit, cleanup_on_start):
        super(HCFStates, self).__init__(cache_size_limit)
        self._hs_client = HubstorageClient(auth=auth)
        self.projectid = project_id
        project = self._hs_client.get_project(self.projectid)
        self._collections = project.collections
        self._colname = colname + "_states"
        self.logger = logging.getLogger("hcf.tates")

        if cleanup_on_start:
            self._cleanup()

    def _cleanup(self):
        while True:
            nextstart = None
            params = {'method':'DELETE',
                      'url':'https://storage.scrapinghub.com/collections/%d/s/%s' % (self.projectid, self._colname),
                      'auth':self._hs_client.auth}
            if nextstart:
                params['prefix'] = nextstart
            response = self._hs_client.session.request(**params)
            if response.status_code != 200:
                self.logger.error("%d %s", response.status_code, response.content)
                self.logger.info(params)
            try:
                r = loads(response.content)
                self.logger.debug("Removed %d, scanned %d", r["deleted"], r["scanned"])
                nextstart = r.get('nextstart')
            except ValueError, ve:
                self.logger.debug(ve)
                self.logger.debug("content: %s (%d)" % (response.content, len(response.content)))
            if not nextstart:
                break

    def frontier_start(self):
        self._store = self._collections.new_store(self._colname)

    def frontier_stop(self):
        self.logger.debug("Got frontier stop.")
        self.flush()
        self._hs_client.close()

    def _hcf_fetch(self, to_fetch):
        finished = False
        i = iter(to_fetch)
        while True:
            prepared_keys = []
            while True:
                try:
                    prepared_keys.append("key=%s" % i.next())
                    if len(prepared_keys) > 32:
                        break
                except StopIteration:
                    finished = True
                    break

            prepared_keys.append("meta=_key")
            params = {'method':'GET',
                      'url':'https://storage.scrapinghub.com/collections/%d/s/%s' % (self.projectid, self._colname),
                      'params':str('&').join(prepared_keys),
                      'auth':self._hs_client.auth}
            start = time()
            response = self._hs_client.session.request(**params)
            self.logger.debug("Fetch request time %f ms", (time()-start) * 1000)
            if response.status_code != 200:
                self.logger.error("%d %s", response.status_code, response.content)
                self.logger.info(params)
            for line in response.content.split('\n'):
                if not line:
                    continue
                try:
                    yield loads(line)
                except ValueError, ve:
                    self.logger.debug(ve)
                    self.logger.debug("content: %s (%d)" % (line, len(line)))
            if finished:
                break

    def fetch(self, fingerprints):
        to_fetch = [f for f in fingerprints if f not in self._cache]
        self.logger.debug("cache size %s" % len(self._cache))
        self.logger.debug("to fetch %d from %d" % (len(to_fetch), len(fingerprints)))
        if not to_fetch:
            return
        count = 0
        for o in self._hcf_fetch(to_fetch):
            self._cache[o['_key']] = o['value']
            count += 1
        self.logger.debug("Fetched %d items" % count)

    def flush(self, force_clear=False):
        buffer = []
        count = 0
        start = time()
        try:
            for fprint, state_val in self._cache.iteritems():
                buffer.append({'_key': fprint, 'value':state_val})
                if len(buffer) > 1024:
                    count += len(buffer)
                    self._store.set(buffer)
                    buffer = []
        finally:
            count += len(buffer)
            self._store.set(buffer)
        self.logger.debug("Send time %f ms", (time()-start) * 1000)
        self.logger.debug("State cache has been flushed: %d items" % count)
        super(HCFStates, self).flush(force_clear)


class HCFClientWrapper(object):

    def __init__(self, auth, project_id, frontier, batch_size=0, flush_interval=30):
        self._hs_client = HubstorageClient(auth=auth)
        self._hcf = self._hs_client.get_project(project_id).frontier
        self._hcf.batch_size = batch_size
        self._hcf.batch_interval = flush_interval
        self._frontier = frontier
        self._links_count = defaultdict(int)
        self._links_to_flush_count = defaultdict(int)
        self._hcf_retries = 10
        self.logger = logging.getLogger("hubstorage-wrapper")

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
                self.logger.error("Could not read from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                      self._hcf_retries))
            except requests_lib.exceptions.ConnectionError:
                self.logger.error("Connection error while reading from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                      self._hcf_retries))
            except requests_lib.exceptions.RequestException:
                self.logger.error("Error while reading from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                      self._hcf_retries))
            sleep(60 * (i + 1))
        return []

    def delete(self, slot, ids):
        for i in range(self._hcf_retries):
            try:
                self._hcf.delete(self._frontier, slot, ids)
                break
            except requests_lib.exceptions.ReadTimeout:
                self.logger.error("Could not delete ids from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                            self._hcf_retries))
            except requests_lib.exceptions.ConnectionError:
                self.logger.error("Connection error while deleting ids from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                            self._hcf_retries))
            except requests_lib.exceptions.RequestException:
                self.logger.error("Error deleting ids from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
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


class HCFQueue(Queue):
    def __init__(self, auth, project_id, frontier, batch_size, flush_interval, slots_count, slot_prefix,
                 cleanup_on_start):
        self.hcf = HCFClientWrapper(auth=auth,
                                    project_id=project_id,
                                    frontier=frontier,
                                    batch_size=batch_size,
                                    flush_interval=flush_interval)
        self.hcf_slots_count = slots_count
        self.hcf_slot_prefix = slot_prefix
        self.logger = logging.getLogger("hcf.queue")
        self.consumed_batches_ids = dict()
        self.partitions = [self.hcf_slot_prefix+str(i) for i in range(0, slots_count)]
        self.partitioner = FingerprintPartitioner(self.partitions)

        if cleanup_on_start:
            for partition_id in self.partitions:
                self.hcf.delete_slot(partition_id)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.hcf.close()

    def get_next_requests(self, max_next_requests, partition_id, **kwargs):
        return_requests = []
        data = True
        while data and len(return_requests) < max_next_requests:
            data = False
            consumed = []
            for batch in self.hcf.read(partition_id, max_next_requests):
                batch_id = batch['id']
                requests = batch['requests']
                data = len(requests) == max_next_requests
                self.logger.debug("got batch %s of size %d from HCF server" % (batch_id, len(requests)))
                for fingerprint, qdata in requests:
                    request = Request(qdata.get('url', fingerprint), **qdata['request'])
                    if request is not None:
                        request.meta.update({
                            'created_at': datetime.utcnow(),
                            'depth': 0,
                        })
                        request.meta.setdefault('scrapy_meta', {})
                        return_requests.append(request)
                consumed.append(batch_id)
            if consumed:
                self.hcf.delete(partition_id, consumed)
        return return_requests

    def schedule(self, batch):
        scheduled = 0
        for _, score, request, schedule in batch:
            if schedule:
                self._process_hcf_link(request, score)
                scheduled += 1
        self.logger.info('scheduled %d links' % scheduled)

    def _process_hcf_link(self, link, score):
        link.meta.pop('origin_is_frontier', None)
        hcf_request = {'fp': getattr(link, 'meta', {}).get('hcf_fingerprint', link.url)}
        qdata = {'request': {}}
        for attr in ('method', 'headers', 'cookies', 'meta'):
            qdata['request'][attr] = getattr(link, attr)
        hcf_request['qdata'] = qdata

        partition_id = self.partitioner.partition(link.meta['fingerprint'])
        slot = self.hcf_slot_prefix + str(partition_id)
        self.hcf.add_request(slot, hcf_request)

    def count(self):
        """
        Calculates lower estimate of items in the queue for all partitions.
        :return: int
        """
        count = 0
        for partition_id in self.partitions:
            for batch in self.hcf.read(partition_id):
                count += len(batch['requests'])
        return count


class HCFBackend(CommonBackend):

    name = 'HCF Backend'

    def __init__(self, manager):
        settings = manager.settings
        self._metadata = MemoryMetadata()
        self._queue = HCFQueue(settings.get('HCF_AUTH', None),
                               settings.get('HCF_PROJECT_ID'),
                               settings.get('HCF_FRONTIER'),
                               settings.get('HCF_PRODUCER_BATCH_SIZE', 10000),
                               settings.get('HCF_PRODUCER_FLUSH_INTERVAL', 30),
                               settings.get('HCF_PRODUCER_NUMBER_OF_SLOTS', 8),
                               settings.get('HCF_PRODUCER_SLOT_PREFIX', ''),
                               settings.get('HCF_CLEANUP_ON_START', False))
        self._states = HCFStates(settings.get('HCF_AUTH', None),
                                 settings.get('HCF_PROJECT_ID'),
                                 settings.get('HCF_FRONTIER'),
                                 20000,
                                 settings.get('HCF_CLEANUP_ON_START', False))
        self.max_iterations = settings.get('HCF_CONSUMER_MAX_BATCHES', 0)
        self.consumer_slot = settings.get('HCF_CONSUMER_SLOT', 0)
        self.iteration = manager.iteration

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    @property
    def metadata(self):
        return self._metadata

    @property
    def queue(self):
        return self._queue

    @property
    def states(self):
        return self._states

    # TODO: we could collect errored pages, and schedule them back to HCF

    def finished(self):
        if self.max_iterations:
            return self.iteration > self.max_iterations
        return super(HCFBackend, self).finished()

    def get_next_requests(self, max_n_requests, **kwargs):
        batch = self.queue.get_next_requests(max_n_requests, self.consumer_slot, **kwargs)
        self.queue_size -= len(batch)
        return batch