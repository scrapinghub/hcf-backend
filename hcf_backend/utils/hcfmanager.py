"""
Script for easy managing of consumer spiders from multiple slots. It checks available slots and schedules
a job for each one.
"""
import re
import json
import random
import logging

from shub_workflow.crawl import CrawlManager

from hcf_backend.utils.hcfpal import HCFPal


logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class HCFSpiderManager(CrawlManager):

    default_max_jobs = 1

    def __init__(self):
        super().__init__()
        self.hcfpal = HCFPal(self.client._hsclient.get_project(self.project_id))

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument('frontier', help='Frontier name')
        self.argparser.add_argument('prefix', help='Slot prefix')

    def workflow_loop(self):
        slot_re = re.compile(rf"{self.args.prefix}\d+")
        available_slots = set(slot for slot in self.hcfpal.get_slots(self.args.frontier) if slot_re.match(slot))

        running_jobs = 0
        states = 'running', 'pending'
        for state in states:
            for job in self.get_project().jobs.list(spider=self.args.spider, state=state, meta='spider_args'):
                frontera_settings_json = json.loads(job['spider_args'].get('frontera_settings_json', '{}'))
                if 'HCF_CONSUMER_SLOT' in frontera_settings_json:
                    slot = frontera_settings_json['HCF_CONSUMER_SLOT']
                    if slot_re.match(slot):
                        available_slots.discard(slot)
                        running_jobs += 1

        available_slots = [slot for slot in available_slots if self.hcfpal.get_slot_count(self.args.frontier, slot) > 0]
        logger.info(f"Available slots: {available_slots!r}")
        if available_slots:
            random.shuffle(available_slots)
            if self.max_running_jobs:
                max_jobs = self.max_running_jobs - running_jobs
                available_slots = available_slots[:max_jobs]
                if not available_slots:
                    logger.info(f"Already running max number of jobs.")

            for slot in available_slots:
                frontera_settings_json = json.dumps({
                    'HCF_CONSUMER_SLOT': slot
                })
                logger.info(f"Will schedule spider job with frontera settings {frontera_settings_json}")
                jobkey = self.schedule_spider(spider_args_override={'frontera_settings_json': frontera_settings_json})
                logger.info(f"Scheduled job {jobkey} with frontera settings {frontera_settings_json}")
            return True
        return bool(running_jobs)
