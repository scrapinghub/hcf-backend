"""
HCF Backend for Frontera Scheduler

Optimizing frontier setting configuration:

# choose either LIFO or FIFO flavour, depending on your needs
BACKEND = 'hcf_backend.HCFFIFOBackend'
# BACKEND = 'hcf_backend.HCFLIFOBackend'

# on consumer spiders add positive DELAY_ON_EMPTY
# this helps to optimize reading of batches from HCF
# however it MUST be 0 (the default value) in case spider
# is not a consumer and instead is seeded by a start request
# DELAY_ON_EMPTY = 30

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

from collections import defaultdict
import datetime
import requests
import time

from hubstorage import HubstorageClient

from frontera.contrib.backends.memory import Backend, MemoryFIFOBackend, MemoryLIFOBackend
from frontera.exceptions import NotConfigured

from .utils import ParameterManager, ScrapyStatsCollectorWrapper, get_scrapy_stats

try:
    from scrapy import log
except ImportError:
    log = None

__all__ = ['HCFFIFOBackend', 'HCFLIFOBackend']

DEFAULT_HCF_PRODUCER_NUMBER_OF_SLOTS = 8
DEFAULT_HCF_PRODUCER_SLOT_PREFIX = ''
DEFAULT_HCF_PRODUCER_BATCH_SIZE = 10000
DEFAULT_HCF_PRODUCER_RESET_FRONTIER = False
DEFAULT_HCF_CONSUMER_SLOT = 0
DEFAULT_HCF_CONSUMER_MAX_BATCHES = 0
DEFAULT_HCF_CONSUMER_MAX_REQUESTS = 0


def _msg(msg, level=None):
    if log:
        log.msg('(HCFBackend) %s' % msg, level or log.INFO)


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
            except requests.exceptions.ReadTimeout:
                _msg("Could not read from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                      self._hcf_retries), log.ERROR)
            except requests.exceptions.ConnectionError:
                _msg("Connection error while reading from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                      self._hcf_retries), log.ERROR)
            except requests.exceptions.RequestException:
                _msg("Error while reading from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                      self._hcf_retries), log.ERROR)
            time.sleep(60 * (i + 1))
        return []

    def delete(self, slot, ids):
        for i in range(self._hcf_retries):
            try:
                self._hcf.delete(self._frontier, slot, ids)
                break
            except requests.exceptions.ReadTimeout:
                _msg("Could not delete ids from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                            self._hcf_retries), log.ERROR)
            except requests.exceptions.ConnectionError:
                _msg("Connection error while deleting ids from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                            self._hcf_retries), log.ERROR)
            except requests.exceptions.RequestException:
                _msg("Error deleting ids from {0}/{1} try {2}/{3}".format(self._frontier, slot, i+1,
                                                                            self._hcf_retries), log.ERROR)
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


class HCFBaseBackend(Backend):
    """
    In order to enable, follow instructions on how to enable crawl frontier scheduler on frontera doc, and set path
    to either flavor of the backend (HCFFIFOBackend or HCFLIFOBackend) to the frontier setting BACKEND. Simple example::

    in mycrawler/settings.py::

    SCHEDULER = 'frontera.contrib.scrapy.schedulers.frontier.FronteraScheduler'
    FRONTERA_SETTINGS = 'mycrawler.frontera_settings'

    in mycrawler/frontier_settings.py::

    BACKEND = 'hcf_backend.HCFLIFOBackend'
    HCF_CONSUMER_MAX_BATCHES = 500
    MAX_NEXT_REQUESTS = 1000

    Spider settings:
    ----------------

    * HCF_AUTH - Hubstorage auth (not required if job run in scrapinghub or configured in scrapy.cfg)
    * HCF_PROJECT_ID - Hubstorage project id (not required if job run in scrapinghub or configured scrapy.cfg)

    If is producer:
    * HCF_PRODUCER_FRONTIER - The frontier where URLs are written.
    * HCF_PRODUCER_SLOT_PREFIX - Prefix to use for slot names.
    * HCF_PRODUCER_NUMBER_OF_SLOTS - Number of write slots to use.
    * HCF_PRODUCER_BATCH_SIZE - How often slot flush should be called. When a slot reaches the number, it is flushed.
    * HCF_PRODUCER_RESET_FRONTIER - Deletes frontier slots if True. defaults to False.

    If is consumer:
    * HCF_CONSUMER_FRONTIER - The frontier where URLs are readed.
    * HCF_CONSUMER_SLOT - Slot from where the spider will read new URLs.
    * HCF_CONSUMER_MAX_BATCHES - Max batches to read from hubstorage.
    * HCF_CONSUMER_MAX_REQUESTS - Max request to be read from hubstorage.
    (note: crawler stops to read from hcf when any of max batches or max requests limit are reached)

    Spider attributes:
    ------------------

    Spider attributes configures crawl frontier and has precedence over the equivalent scrapy settings. Available
    attributes can be found in scrapy_spider_settings class attribute below

    Spider callbacks:
    -----------------

    * cf_make_request(fingerprint, qdata, request_cls) - Custom build of request from the frontier data. It must return None or an
            instance of the class specified in request_cls. If returns None, the request is ignored. Used in consumer spider.
    * get_producer_slot(request) - Custom mapping from a request to a slot name where request must be stored. It receives an instance
            of the class given by REQUEST_MODEL frontier setting. Used in producer spider.

    """

    scrapy_spider_settings = (
        'hcf_producer_frontier',
        'hcf_consumer_frontier',
        'hcf_producer_number_of_slots',
        'hcf_producer_slot_prefix',
        'hcf_producer_batch_size',
        'hcf_producer_reset_frontier',
        'hcf_consumer_slot',
        'hcf_consumer_max_batches',
        'hcf_consumer_max_requests',
    )

    def __init__(self, manager):
        super(HCFBaseBackend, self).__init__(manager)
        self.manager = manager

        params = ParameterManager(manager)

        self.hs_auth = params.get_from_all('auth', 'HCF_AUTH', required=True)
        self.hs_project_id = params.get_from_all('project_id', 'HCF_PROJECT_ID', required=True)

        self.hcf_producer_frontier = params.get_from_all_settings('HCF_PRODUCER_FRONTIER')
        self.hcf_producer_slot_prefix = params.get_from_all_settings('HCF_PRODUCER_SLOT_PREFIX',
                                                                     default=DEFAULT_HCF_PRODUCER_SLOT_PREFIX)
        self.hcf_producer_number_of_slots = params.get_from_all_settings('HCF_PRODUCER_NUMBER_OF_SLOTS',
                                                                         default=DEFAULT_HCF_PRODUCER_NUMBER_OF_SLOTS)
        self.hcf_producer_batch_size = params.get_from_all_settings('HCF_PRODUCER_BATCH_SIZE',
                                                                    default=DEFAULT_HCF_PRODUCER_BATCH_SIZE)
        self.hcf_producer_reset_frontier = params.get_from_all_settings('HCF_PRODUCER_RESET_FRONTIER',
                                                                        default=DEFAULT_HCF_PRODUCER_RESET_FRONTIER)
        self.hcf_consumer_frontier = params.get_from_all_settings('HCF_CONSUMER_FRONTIER')
        self.hcf_consumer_slot = params.get_from_all_settings('HCF_CONSUMER_SLOT',
                                                              default=DEFAULT_HCF_CONSUMER_SLOT)
        self.hcf_consumer_max_batches = params.get_from_all_settings('HCF_CONSUMER_MAX_BATCHES',
                                                                     default=DEFAULT_HCF_CONSUMER_MAX_BATCHES)
        self.hcf_consumer_max_requests = params.get_from_all_settings('HCF_CONSUMER_MAX_REQUESTS',
                                                                    default=DEFAULT_HCF_CONSUMER_MAX_REQUESTS)

        self.stats = self._get_stats()
        self.n_consumed_batches = 0
        self.n_consumed_requests = 0
        self.producer_get_slot_callback = self._get_producer_slot

        self._init_roles()

    def frontier_start(self, **kwargs):
        super(HCFBaseBackend, self).frontier_start(**kwargs)
        scrapy_spider = kwargs.get('spider', None)
        if scrapy_spider:
            self._copy_spider_settings(scrapy_spider)
            # roles might have changed.
            self._init_roles()
            self.producer_get_slot_callback = getattr(scrapy_spider, 'get_producer_slot',
                                                      self.producer_get_slot_callback)
            self.make_request = getattr(scrapy_spider, 'cf_make_request', self._make_request)

        self._log_start_message()

        if self.producer and self.hcf_producer_reset_frontier:
            self._reset_producer_frontier()

    def frontier_stop(self, **kwargs):
        super(HCFBaseBackend, self).frontier_stop(**kwargs)

        if self.producer:
            n_flushed_links = self.producer.flush()
            if n_flushed_links:
                _msg('Flushing %d link(s) to all slots' % n_flushed_links)
            self.producer.close()

        if self.consumer:
            self.consumer.close()

    def _get_or_create_request(self, request):
        if self._is_hcf(request):
            assert self.producer, 'HCF request received but backend is not defined as producer'
            self._process_hcf_response_link(response, link)
            return None, False # already processed
        return super(HCFBaseBackend, self)._get_or_create_request(request)
        
    def get_next_requests(self, max_next_requests, **kwargs):
        if self.hcf_consumer_max_requests > 0:
            max_next_requests = min(max_next_requests, self.hcf_consumer_max_requests - self.n_consumed_requests)
        if self.consumer and not (self._consumer_max_batches_reached() or self._consumer_max_requests_reached()):
            n_queued_requests = len(self.heap)
            n_remaining_requests = max_next_requests - n_queued_requests
            if n_remaining_requests > 0:
                for request in self._get_requests_from_hs(n_remaining_requests):
                    self.heap.push(request)
        return super(HCFBaseBackend, self).get_next_requests(max_next_requests)

    def _copy_spider_settings(self, scrapy_spider):
        for attr in self.scrapy_spider_settings:
            if hasattr(scrapy_spider, attr):
                attrval = getattr(scrapy_spider, attr)
                if attr in ('hcf_producer_number_of_slots',
                            'hcf_producer_batch_size',
                            'hcf_consumer_max_batches',
                            'hcf_consumer_max_requests'):
                    attrval = int(attrval)
                setattr(self, attr, attrval)

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
                    request = self.make_request(fingerprint, qdata, self.manager.request_model)
                    if request is not None:
                        request.meta.update({
                            'created_at': datetime.datetime.utcnow(),
                            'depth': 0,
                        })
                        return_requests.append(request)
                        self.n_consumed_requests += 1
                consumed_batches_ids.append(batch_id)
                self.stats.inc_value(self._get_consumer_stats_msg('batches'))
                _msg('Reading %d request(s) from batch %s ' % (len(requests), batch_id))

            if consumed_batches_ids:
                self.consumer.delete(self.hcf_consumer_slot, consumed_batches_ids)
                self.n_consumed_batches += len(consumed_batches_ids)


        return return_requests

    def _make_request(self, fingerprint, qdata, request_cls):
        url = qdata.get('url', fingerprint)
        return self.manager.make_request(url)

    def _get_stats(self):
        scrapy_stats = get_scrapy_stats(self.manager.extra)
        return ScrapyStatsCollectorWrapper(scrapy_stats)

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
        _msg('HCF project: %s' % self.hs_project_id)
        _msg('HCF producer: %s' % producer_message)
        _msg('HCF consumer: %s' % consumer_message)

    def _process_hcf_response_link(self, response, link):
        if link.method != 'GET':
            _msg("HCF does not support non GET requests (%s)" % link.url, log.ERROR)
            return

        slot = self.producer_get_slot_callback(link)

        hcf_request = link.meta.get('hcf_request', {})
        hcf_request.setdefault('fp', link.url)
        hcf_request.setdefault('qdata', {})

        n_flushed_links = self.producer.add_request(slot, hcf_request)
        if n_flushed_links:
            _msg('Flushing %d link(s) to slot %s' % (n_flushed_links, slot))

        self.stats.inc_value(self._get_producer_stats_msg(slot))
        self.stats.inc_value(self._get_producer_stats_msg())

    def _is_hcf(self, request_or_response):
        return request_or_response.meta.get('cf_store', False)

    def _consumer_max_batches_reached(self):
        if not self.hcf_consumer_max_batches:
            return False
        return self.n_consumed_batches >= self.hcf_consumer_max_batches

    def _consumer_max_requests_reached(self):
        if not self.hcf_consumer_max_requests:
            return False
        return self.n_consumed_requests >= self.hcf_consumer_max_requests

    def _init_roles(self):
        self.producer = None
        self.consumer = None
        if self.hcf_producer_frontier:
            self.producer = HCFManager(auth=self.hs_auth,
                                       project_id=self.hs_project_id,
                                       frontier=self.hcf_producer_frontier,
                                       batch_size=self.hcf_producer_batch_size)
            self.stats.set_value(self._get_producer_stats_msg(), 0)
        if self.hcf_consumer_frontier:
            self.consumer = HCFManager(auth=self.hs_auth,
                                       project_id=self.hs_project_id,
                                       frontier=self.hcf_consumer_frontier)
            self.stats.set_value(self._get_consumer_stats_msg(), 0)

    def _get_producer_slot(self, request):
        """Determine to which slot should be saved the request.

        This provides a default implementation that distributes urls among the
        available number of slots based in the URL hash.

        Depending on the urls, this distribution might or not be evenly among
        the slots.

        This method must return a string value for the slot, and preferably be
        well defined, that is, return the same slot for the same request.
        """
        if 'hcf_producer_slot' in request.meta:
            return request.meta['hcf_producer_slot']

        # Allow to specify the number of slots per-request basis.
        n_slots = request.meta.get('hcf_producer_number_of_slots', self.hcf_producer_number_of_slots)

        fingerprint = request.meta['fingerprint']
        slotno = str(int(fingerprint, 16) % n_slots)
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

    def _reset_producer_frontier(self):
        _msg('reseting producer slots...')
        for slot in range(self.hcf_producer_number_of_slots):
            slot_name = self._get_producer_slot_name(slot)
            _msg('deleting producer slot %s' % slot_name)
            self.producer.delete_slot(slot_name)


class HCFFIFOBackend(HCFBaseBackend, MemoryFIFOBackend):
    component_name = 'HCF FIFO Memory Backend'


class HCFLIFOBackend(HCFBaseBackend, MemoryLIFOBackend):
    component_name = 'HCF LIFO Memory Backend'
