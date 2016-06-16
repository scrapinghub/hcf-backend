"""
HCF Backend for Frontera Scheduler

Optimizing frontier setting configuration:

BACKEND = 'hcf_backend.HCFBackend'

# If you want to limit each consumer job, use one of the
# following parameters. One limits by read requests count,
# the other by read batches count. In HCF each batch contains
# no more than 100 requests.
# HCF_CONSUMER_MAX_REQUESTS = 15000
# HCF_CONSUMER_MAX_BATCHES = 150

# How many requests are read on each call to HCF. If 0, limit
# is determined by the speed at which scrapy demands requests, which
# is very unnefficient because it makes lots of calls to HCF which retrieves
# a small number of requests. Same if you give it a small value. A too big value
# will make to retrieve a total of requests/batches bigger than setted
# in HCF_CONSUMER_MAX_* settings. Can also make to fail requests to HCF
# by timeout. A value of 1000 is optimal for most purposes.
MAX_NEXT_REQUESTS = 1000

Read class docstring below for details on other configuration settings.
"""

import datetime
import logging

from frontera import Backend

from hcf_backend.manager import HCFManager


__all__ = ['HCFBackend']

LOG = logging.getLogger(__name__)


DEFAULT_HCF_PRODUCER_NUMBER_OF_SLOTS = 8
DEFAULT_HCF_PRODUCER_SLOT_PREFIX = ''
DEFAULT_HCF_PRODUCER_BATCH_SIZE = 10000
DEFAULT_HCF_CONSUMER_SLOT = 0
DEFAULT_HCF_CONSUMER_MAX_BATCHES = 0
DEFAULT_HCF_CONSUMER_MAX_REQUESTS = 0


class HCFBackend(Backend):
    """
    In order to enable, follow instructions on how to enable crawl frontier scheduler on frontera doc, and set
    frontier BACKEND setting to hcf_backend.HCFBackend. Simple example::

    in mycrawler/settings.py::

    SCHEDULER = 'frontera.contrib.scrapy.schedulers.frontier.FronteraScheduler'
    FRONTERA_SETTINGS = 'mycrawler.frontera_settings'

    in mycrawler/frontier_settings.py::

    BACKEND = 'hcf_backend.HCFBackend'
    HCF_CONSUMER_MAX_BATCHES = 500
    MAX_NEXT_REQUESTS = 1000

    Backend settings:
    -----------------

    * HCF_AUTH - Hubstorage auth (not required if job run in scrapinghub or configured in scrapy.cfg)
    * HCF_PROJECT_ID - Hubstorage project id (not required if job run in scrapinghub or configured scrapy.cfg)

    If is producer:
    * HCF_PRODUCER_FRONTIER - The frontier where URLs are written.
    * HCF_PRODUCER_SLOT_PREFIX - Prefix to use for slot names.
    * HCF_PRODUCER_NUMBER_OF_SLOTS - Number of write slots to use.
    * HCF_PRODUCER_BATCH_SIZE - How often slot flush should be called. When a slot reaches the number, it is flushed.
    * HCF_GET_PRODUCER_SLOT(request) - Custom mapping from a request to a slot name where request must be stored. It receives an instance
            of the class given by REQUEST_MODEL frontier setting.


    If is consumer:
    * HCF_CONSUMER_FRONTIER - The frontier where URLs are readed.
    * HCF_CONSUMER_SLOT - Slot from where the spider will read new URLs.
    * HCF_CONSUMER_MAX_BATCHES - Max batches to read from hubstorage.
    * HCF_CONSUMER_MAX_REQUESTS - Max request to be read from hubstorage.
        (note: crawler stops to read from hcf when any of max batches or max requests limit are reached)
    * HCF_MAKE_REQUEST(fingerprint, qdata, request_cls) - Custom build of request from the frontier data. It must return None or an
            instance of the class specified in request_cls. If returns None, the request is ignored. Used in consumer spider.

    """

    backend_settings = (
        'HCF_AUTH',
        'HCF_PROJECT_ID',

        'HCF_PRODUCER_FRONTIER',
        'HCF_PRODUCER_SLOT_PREFIX',
        'HCF_PRODUCER_NUMBER_OF_SLOTS',
        'HCF_PRODUCER_BATCH_SIZE',
        'HCF_GET_PRODUCER_SLOT',

        'HCF_CONSUMER_FRONTIER',
        'HCF_CONSUMER_SLOT',
        'HCF_CONSUMER_MAX_BATCHES',
        'HCF_CONSUMER_MAX_REQUESTS',
        'HCF_MAKE_REQUEST',
    )

    component_name = 'HCF Backend'

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def __init__(self, manager):
        self.manager = manager

        self.hcf_auth = None
        self.hcf_project_id = None

        self.hcf_producer_frontier = None
        self.hcf_producer_slot_prefix = DEFAULT_HCF_PRODUCER_SLOT_PREFIX
        self.hcf_producer_number_of_slots = DEFAULT_HCF_PRODUCER_NUMBER_OF_SLOTS
        self.hcf_producer_batch_size = DEFAULT_HCF_PRODUCER_BATCH_SIZE
        self.hcf_get_producer_slot = self._producer_get_slot_callback

        self.hcf_consumer_frontier = None
        self.hcf_consumer_slot = DEFAULT_HCF_CONSUMER_SLOT
        self.hcf_consumer_max_batches = DEFAULT_HCF_CONSUMER_MAX_BATCHES
        self.hcf_consumer_max_requests = DEFAULT_HCF_CONSUMER_MAX_REQUESTS
        self.hcf_make_request = self._make_request

        self.stats = self.manager.settings.get('STATS_MANAGER')

        self.n_consumed_batches = 0
        self.n_consumed_requests = 0

        self.producer = None
        self.consumer = None


    def frontier_start(self):
        for attr in self.backend_settings:
            value = self.manager.settings.get(attr)
            if value is not None:
                setattr(self, attr.lower(), value)
        self._init_roles()
        self._log_start_message()

    def frontier_stop(self):
        if self.producer:
            n_flushed_links = self.producer.flush()
            if n_flushed_links:
                LOG.info('Flushing %d link(s) to all slots' % n_flushed_links)
            self.producer.close()

        if self.consumer:
            self.consumer.close()

    def add_seeds(self, seeds):
        pass

    def page_crawled(self, response, links):
        if links:
            assert self.producer, 'HCF request received but backend is not configured as producer'
            for request in links:
                self._process_hcf_link(request)

    def get_next_requests(self, max_next_requests, **kwargs):

        requests = []

        if self.hcf_consumer_max_requests > 0:
            max_next_requests = min(max_next_requests, self.hcf_consumer_max_requests - self.n_consumed_requests)

        if self.consumer and not (self._consumer_max_batches_reached() or self._consumer_max_requests_reached()) \
                    and max_next_requests:
            for request in self._get_requests_from_hs(max_next_requests):
                requests.append(request)

        return requests

    def _get_requests_from_hs(self, n_min_requests):
        return_requests = []
        data = True

        while data and len(return_requests) < n_min_requests and \
                    not (self._consumer_max_batches_reached() or self._consumer_max_requests_reached()):
            consumed_batches_ids = []
            data = False
            for batch in self.consumer.read(self.hcf_consumer_slot, n_min_requests):
                data = True
                batch_id = batch['id']
                requests = batch['requests']
                self.stats.inc_value(self._get_consumer_stats_msg('requests'), len(requests))
                for fingerprint, qdata in requests:
                    request = self.hcf_make_request(fingerprint, qdata, self.manager.request_model)
                    if request is not None:
                        request.meta.update({
                            'created_at': datetime.datetime.utcnow(),
                            'depth': 0,
                        })
                        request.meta.setdefault('scrapy_meta', {})
                        return_requests.append(request)
                        self.n_consumed_requests += 1
                consumed_batches_ids.append(batch_id)
                self.stats.inc_value(self._get_consumer_stats_msg('batches'))
                LOG.info('Reading %d request(s) from batch %s ' % (len(requests), batch_id))

            if consumed_batches_ids:
                self.consumer.delete(self.hcf_consumer_slot, consumed_batches_ids)
                self.n_consumed_batches += len(consumed_batches_ids)


        return return_requests

    def _make_request(self, fingerprint, qdata, request_cls):
        url = qdata.get('url', fingerprint)
        return request_cls(url, **qdata['request'])

    def _log_start_message(self):
        producer_message = 'NO'
        consumer_message = 'NO'
        if self.producer:
            if self.hcf_producer_number_of_slots > 1:
                slots_message = '[0-%d]' % (self.hcf_producer_number_of_slots-1)
            else:
                slots_message = '0'
            producer_message = '%s/%s%s' % (self.hcf_producer_frontier,
                                            self.hcf_producer_slot_prefix,
                                            slots_message)
        if self.consumer:
            consumer_message = '%s/%s' % (self.hcf_consumer_frontier,
                                          self.hcf_consumer_slot)
        LOG.info('HCF project: %s' % self.hcf_project_id)
        LOG.info('HCF producer: %s' % producer_message)
        LOG.info('HCF consumer: %s' % consumer_message)

    def _process_hcf_link(self, link):
        link.meta.pop('origin_is_frontier', None)
        hcf_request = {'fp': getattr(link, 'meta', {}).get('frontier_fingerprint', link.url)}
        qdata = {'request': {}}
        for attr in ('url', 'method', 'headers', 'cookies', 'meta'):
            qdata['request'][attr] = getattr(link, attr)
        hcf_request['qdata'] = qdata

        slot = self.hcf_get_producer_slot(link)
        n_flushed_links = self.producer.add_request(slot, hcf_request)
        if n_flushed_links:
            LOG.info('Flushing %d link(s) to slot %s' % (n_flushed_links, slot))

        self.stats.inc_value(self._get_producer_stats_msg(slot))
        self.stats.inc_value(self._get_producer_stats_msg())

    def _consumer_max_batches_reached(self):
        if not self.hcf_consumer_max_batches:
            return False
        return self.n_consumed_batches >= self.hcf_consumer_max_batches

    def _consumer_max_requests_reached(self):
        if not self.hcf_consumer_max_requests:
            return False
        return self.n_consumed_requests >= self.hcf_consumer_max_requests

    def _init_roles(self):

        if self.hcf_producer_frontier:
            self.producer = HCFManager(auth=self.hcf_auth,
                                       project_id=self.hcf_project_id,
                                       frontier=self.hcf_producer_frontier,
                                       batch_size=self.hcf_producer_batch_size)

        if self.hcf_consumer_frontier:
            self.consumer = HCFManager(auth=self.hcf_auth,
                                       project_id=self.hcf_project_id,
                                       frontier=self.hcf_consumer_frontier)

    def _producer_get_slot_callback(self, request):
        """Determine to which slot should be saved the request.

        This provides a default implementation that distributes urls among the
        available number of slots based in the URL hash.

        Depending on the urls, this distribution might or not be evenly among
        the slots.

        This method must return a string value for the slot, and preferably be
        well defined, that is, return the same slot for the same request.
        """
        fingerprint = request.meta['fingerprint']
        slotno = str(int(fingerprint, 16) % self.hcf_producer_number_of_slots)
        slot = self._get_producer_slot_name(slotno)
        return slot

    def _get_consumer_stats_msg(self, msg=None):
        stats_msg = 'hcf/consumer/%s/%s' % (self.hcf_consumer_frontier, self.hcf_consumer_slot)
        if msg:
            stats_msg += '/%s' % msg
        return stats_msg

    def _get_producer_stats_msg(self, slot=None, msg=None):
        stats_msg = 'hcf/producer/%s' % (self.hcf_producer_frontier)
        if slot:
            stats_msg += '/%s' % slot
        if msg:
            stats_msg += '/%s' % msg
        return stats_msg

    def _get_producer_slot_name(self, slotno):
        return self.hcf_producer_slot_prefix + str(slotno)

    def request_error(self, request, error):
        pass
