"""
HCF Backend for Frontera Scheduler

Optimizing frontier setting configuration:
------------------------------------------

If you want to limit each consumer job, use one of the
following parameters. One limits by read requests count,
the other by read batches count. In HCF each batch contains
no more than 100 requests.
HCF_CONSUMER_MAX_REQUESTS = 15000
HCF_CONSUMER_MAX_BATCHES = 150

How many requests are read on each call to HCF. If 0, limit
is determined by the speed at which scrapy demands requests, which
is very unnefficient because it makes lots of calls to HCF which retrieves
a small number of requests. Same if you give it a small value. A too big value
will make to retrieve a total of requests/batches bigger than setted
in HCF_CONSUMER_MAX_* settings. Can also make to fail requests to HCF
by timeout. A value of 1000 is optimal for most purposes.
MAX_NEXT_REQUESTS = 1000

Another important setting that affects performance is HCF_PRODUCER_BATCH_SIZE. This is the number
of requests accumulated per slot before syncing to the frontier. Hence, this setting affects
the memory needed for the producer. The default of 1000 is good enough for most purposes. But if
you still have memory issues, specially when using many slots, you may want to decrease it.

Usage details
-------------

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

* HCF_AUTH - Hubstorage auth (not required if SH_APIKEY environment variable is set up)
* HCF_PROJECT_ID - Hubstorage project id (not required if job run in scrapinghub ScrapyCloud, or PROJECT_ID
                   environment variable is set up)

If is producer:
* HCF_PRODUCER_FRONTIER - The frontier where URLs are written.
* HCF_PRODUCER_SLOT_PREFIX - Prefix to use for slot names.
* HCF_PRODUCER_NUMBER_OF_SLOTS - Number of write slots to use.
* HCF_PRODUCER_BATCH_SIZE - How often slot flush should be called. When a slot reaches the number, it is flushed.
  HCF max batch size is 100, so if HCF_PRODUCER_BATCH_SIZE is greater than 100, will result on more than one HCF batches
  of up to 100 requests each. If HCF_PRODUCER_BATCH_SIZE is lower than 100, will result on batches up to the given size.


If is consumer:
* HCF_CONSUMER_FRONTIER - The frontier where URLs are readed.
* HCF_CONSUMER_SLOT - Slot from where the spider will read new URLs.
* HCF_CONSUMER_MAX_BATCHES - Max batches to read from hubstorage.
* HCF_CONSUMER_MAX_REQUESTS - Max request to be read from hubstorage.
    (note: crawler stops to read from hcf when any of max batches or max requests limit are reached)
* HCF_CONSUMER_DONT_DELETE_REQUESTS - If given and True, don't delete requests from frontier once read.
  For testing purposes or delegation of batch control.
* HCF_CONSUMER_DELETE_BATCHES_ON_STOP - If given and True, read batches will be deleted when the job finishes.
    Default is to delete batches once read.

"""

import datetime
import logging

from frontera import Backend
from shub_workflow.utils import resolve_project_id

from .manager import HCFManager
from .utils import (
    convert_from_bytes,
    convert_to_bytes,
    assign_slotno,
    get_apikey
)


__all__ = ['HCFBackend']

LOG = logging.getLogger(__name__)


DEFAULT_HCF_PRODUCER_NUMBER_OF_SLOTS = 1
DEFAULT_HCF_PRODUCER_SLOT_PREFIX = ''
DEFAULT_HCF_PRODUCER_BATCH_SIZE = 1000
DEFAULT_HCF_CONSUMER_SLOT = '0'
DEFAULT_HCF_CONSUMER_MAX_BATCHES = 0
DEFAULT_HCF_CONSUMER_MAX_REQUESTS = 0


class HCFBackend(Backend):

    backend_settings = (
        'HCF_AUTH',
        'HCF_PROJECT_ID',

        'HCF_PRODUCER_FRONTIER',
        'HCF_PRODUCER_SLOT_PREFIX',
        'HCF_PRODUCER_NUMBER_OF_SLOTS',
        'HCF_PRODUCER_BATCH_SIZE',

        'HCF_CONSUMER_FRONTIER',
        'HCF_CONSUMER_SLOT',
        'HCF_CONSUMER_MAX_BATCHES',
        'HCF_CONSUMER_MAX_REQUESTS',
        'HCF_CONSUMER_DONT_DELETE_REQUESTS',
        'HCF_CONSUMER_DELETE_BATCHES_ON_STOP',
    )

    component_name = 'HCF Backend'

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def __init__(self, manager):
        self.manager = manager

        self.hcf_auth = get_apikey()
        self.hcf_project_id = resolve_project_id()

        self.hcf_producer_frontier = None
        self.hcf_producer_slot_prefix = DEFAULT_HCF_PRODUCER_SLOT_PREFIX
        self.hcf_producer_number_of_slots = DEFAULT_HCF_PRODUCER_NUMBER_OF_SLOTS
        self.hcf_producer_batch_size = DEFAULT_HCF_PRODUCER_BATCH_SIZE

        self.hcf_consumer_frontier = None
        self.hcf_consumer_slot = DEFAULT_HCF_CONSUMER_SLOT
        self.hcf_consumer_max_batches = DEFAULT_HCF_CONSUMER_MAX_BATCHES
        self.hcf_consumer_max_requests = DEFAULT_HCF_CONSUMER_MAX_REQUESTS
        self.hcf_consumer_dont_delete_requests = False
        self.hcf_consumer_delete_batches_on_stop = False

        self.stats = self.manager.settings.get('STATS_MANAGER')

        self.n_consumed_batches = 0
        self.n_consumed_requests = 0

        self.producer = None
        self.consumer = None

        self.consumed_batches_ids = []
        self._no_last_data = False

    def frontier_start(self):
        for attr in self.backend_settings:
            value = self.manager.settings.get(attr)
            if value is not None:
                setattr(self, attr.lower(), value)
        try:
            int(self.hcf_project_id)
        except TypeError:
            LOG.warning("Could not detect project. You must set HCF_PROJECT_ID setting. HCFBackend is not configured.")
        else:
            self._init_roles()
            self._log_start_message()

    def _get_producer_newcounts(self):
        try:
            newcount = self.producer._hcf.newcount
        except Exception:
            pass
        else:
            yield None, newcount

        # TODO: add per-slot newcounts in python-scrapinghub & here.

    def _update_producer_new_links_stat(self):
        for slot, newcount in self._get_producer_newcounts():
            self.stats.set_value(
                self._get_producer_stats_msg(slot=slot, msg='new_links'),
                newcount
            )

    def frontier_stop(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            self._update_producer_new_links_stat()

        if self.consumer:
            if not self.hcf_consumer_dont_delete_requests:
                self.delete_read_batches()

            self.consumer.close()

    def add_seeds(self, seeds):
        for request in seeds:
            self._process_hcf_link(request)
        self._update_producer_new_links_stat()

    def page_crawled(self, response):
        pass

    def links_extracted(self, request, links):
        for req in links:
            self._process_hcf_link(req)
        self._update_producer_new_links_stat()

    def get_next_requests(self, max_next_requests, **kwargs):

        requests = []

        if self.hcf_consumer_max_requests > 0:
            max_next_requests = min(max_next_requests, self.hcf_consumer_max_requests - self.n_consumed_requests)

        if self.consumer and not (self._consumer_max_batches_reached() or self._consumer_max_requests_reached()) \
                and max_next_requests:
            for request in self._get_requests_from_hs(max_next_requests):
                requests.append(request)

        return requests

    @staticmethod
    def _convert_qdata_to_bytes(qdata):
        req = qdata['request']
        req['headers'] = convert_to_bytes(req.get('headers', {}))
        req['cookies'] = convert_to_bytes(req.get('cookies', {}))
        for key in list(req.get('meta', {}).keys()):
            req['meta'][key.encode('utf8')] = req['meta'].pop(key)

    def _get_requests_from_hs(self, n_min_requests):
        return_requests = []
        data = True

        if self.producer is not None:
            self.producer.flush()

        while data and len(return_requests) < n_min_requests and \
                not (self._consumer_max_batches_reached() or self._consumer_max_requests_reached()):
            data = False
            for batch in self.consumer.read(self.hcf_consumer_slot, n_min_requests):
                data = True
                batch_id = batch['id']
                if batch_id in self.consumed_batches_ids:
                    return return_requests
                requests = batch['requests']
                self.stats.inc_value(self._get_consumer_stats_msg('requests'), len(requests))
                for fingerprint, qdata in requests:
                    self._convert_qdata_to_bytes(qdata)
                    request = self._make_request(fingerprint, qdata)
                    if request is not None:
                        request.meta.update({
                            b'created_at': datetime.datetime.utcnow(),
                            b'depth': 0,
                        })
                        return_requests.append(request)
                        self.n_consumed_requests += 1
                self.consumed_batches_ids.append(batch_id)
                self.n_consumed_batches += 1
                self.stats.inc_value(self._get_consumer_stats_msg('batches'))
                LOG.info('Reading %d request(s) from batch %s ', len(requests), batch_id)

            if not self.hcf_consumer_dont_delete_requests and not self.hcf_consumer_delete_batches_on_stop:
                self.delete_read_batches()

        self._no_last_data = not data
        return return_requests

    def delete_read_batches(self):
        if self.consumed_batches_ids:
            self.consumer.delete(self.hcf_consumer_slot, self.consumed_batches_ids)
            LOG.info('Deleting read batches: %s', self.consumed_batches_ids)
        self.consumed_batches_ids = []

    def _make_request(self, fingerprint, qdata):
        kwargs = qdata['request']
        kwargs['meta'][b'frontier_fingerprint'] = fingerprint
        return self.manager.request_model(qdata['url'], **kwargs)

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
        LOG.info('HCF project: %s', self.hcf_project_id)
        LOG.info('HCF producer: %s', producer_message)
        LOG.info('HCF consumer: %s', consumer_message)

    def _process_hcf_link(self, link):
        if not self.producer:
            LOG.debug(f'Ignoring {link.url}: backend not configured as producer')
            return
        link.meta.pop(b'origin_is_frontier', None)
        hcf_request = {'fp': link.meta[b'frontier_fingerprint']}
        qdata = {'request': {}}
        for attr in ('method', 'headers', 'cookies', 'meta'):
            qdata['request'][attr] = getattr(link, attr)
        qdata['url'] = link.url
        hcf_request['qdata'] = qdata

        slot = self.hcf_get_producer_slot(link)
        if slot:
            self.producer.add_request(slot, convert_from_bytes(hcf_request))

            self.stats.inc_value(self._get_producer_stats_msg(slot, msg='total_links'))
            self.stats.inc_value(self._get_producer_stats_msg(msg='total_links'))

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

        if self.hcf_consumer_frontier and self.hcf_consumer_slot:
            self.consumer = HCFManager(auth=self.hcf_auth,
                                       project_id=self.hcf_project_id,
                                       frontier=self.hcf_consumer_frontier)

    def hcf_get_producer_slot(self, request):
        """Determine to which slot should be saved the request.

        Depending on the urls, this distribution might or not be evenly among
        the slots.
        """
        fingerprint = request.meta[b'frontier_fingerprint']
        slot_prefix = request.meta.get(b'frontier_slot_prefix', self.hcf_producer_slot_prefix)
        num_slots = request.meta.get(b'frontier_number_of_slots', self.hcf_producer_number_of_slots)
        slotno = assign_slotno(fingerprint, num_slots)
        slot = slot_prefix + str(slotno)
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

    def request_error(self, request, error):
        pass

    def finished(self):
        return self._no_last_data and (self.producer is None or self.producer.get_number_of_links_to_flush() == 0)
