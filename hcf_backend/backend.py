# -*- coding: utf-8 -*-

from frontera.core.components import Queue, Backend
from frontera.core.models import Request
from manager import HubstorageCrawlFrontier

from datetime import datetime
from frontera.contrib.backends.partitioners import FingerprintPartitioner


class HCFQueue(Queue):
    def __init__(self, logger, auth, project_id, frontier, batch_size, slots_count, slot_prefix):
        self.hcf = HubstorageCrawlFrontier(auth=auth,
                                           project_id=project_id,
                                           frontier=frontier,
                                           batch_size=batch_size)
        self.hcf_slots_count = slots_count
        self.hcf_slot_prefix = slot_prefix
        self.logger = logger
        self.consumed_batches_ids = dict()
        self.partitions = [i for i in range(0, slots_count)]
        self.partitioner = FingerprintPartitioner(self.partitions)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.hcf.flush()
        for slot, batches in self.consumed_batches_ids:
            self.hcf.delete(slot, batches)
        self.hcf.close()

    def get_next_requests(self, max_next_requests, partition_id, **kwargs):
        return_requests = []
        data = True
        while data and len(return_requests) < max_next_requests:
            data = False
            for batch in self.hcf.read(partition_id, max_next_requests):
                batch_id = batch['id']
                requests = batch['requests']
                data = len(requests) == max_next_requests
                self.logger.debug("got batch %d of size %d from HCF server" % (batch_id, len(requests)))
                for fingerprint, qdata in requests:
                    request = Request(qdata.get('url', fingerprint), **qdata['request'])
                    if request is not None:
                        request.meta.update({
                            'created_at': datetime.utcnow(),
                            'depth': 0,
                        })
                        request.meta.setdefault('scrapy_meta', {})
                        return_requests.append(request)
                self.consumed_batches_ids[partition_id].append(batch_id)
        return return_requests

    def schedule(self, batch):
        for _, score, request, schedule in batch:
            if schedule:
                self._process_hcf_link(request, score)

    def _process_hcf_link(self, link, score):
        link.meta.pop('origin_is_frontier', None)
        hcf_request = {'fp': getattr(link, 'meta', {}).get('hcf_fingerprint', link.url)}
        qdata = {'request': {}}
        for attr in ('method', 'headers', 'cookies', 'meta'):
            qdata['request'][attr] = getattr(link, attr)
        hcf_request['qdata'] = qdata

        partition_id = self.partitioner.partition(link.meta['fingerprint'])
        slot = self.hcf_slot_prefix + str(partition_id)
        n_flushed_links = self.hcf.add_request(slot, hcf_request)
        if n_flushed_links:
            self.logger.info('flushing %d link(s) to slot %s' % (n_flushed_links, slot))

    def count(self):
        pass


class HCFBackend(Backend):

    name = 'HCF Backend'

    def __init__(self, manager):
        settings = manager.settings
        self._metadata = None
        self._queue = HCFQueue(manager.logger.backend,
                               settings.get('HCF_AUTH', None),
                               settings.get('HCF_PROJECT_ID'),
                               settings.get('HCF_FRONTIER'),
                               settings.get('HCF_PRODUCER_BATCH_SIZE', 10000),
                               settings.get('HCF_PRODUCER_NUMBER_OF_SLOTS', 8),
                               settings.get('HCF_PRODUCER_SLOT_PREFIX', ''))
        self._states = None
        self.max_iterations = settings.get('HCF_CONSUMER_MAX_BATCHES', 0)
        self.consumer_slot = settings.get('HCF_CONSUMER_SLOT', 0)
        self.iteration = manager.iteration

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    @property
    def states(self):
        return self._states

    @property
    def metadata(self):
        return self._metadata

    @property
    def queue(self):
        return self._queue

    def frontier_stop(self):
        pass

    def frontier_start(self):
        pass

    def add_seeds(self, seeds):
        pass

    def page_crawled(self, response, links):
        pass

    def request_error(self, page, error):
        # TODO: we could collect errored pages, and schedule them back to HCF
        pass

    def finished(self):
        return self.iteration > self.max_iterations

    def get_next_requests(self, max_n_requests, **kwargs):
        return self.queue.get_next_requests(max_n_requests, self.consumer_slot, **kwargs)
