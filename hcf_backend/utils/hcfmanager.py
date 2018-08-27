"""
Script for easy managing of consumer spiders from multiple slots. It checks available slots and schedules
a job for each one.
"""
import re
import time
import json
import random
import logging
import argparse

from scrapinghub import ScrapinghubClient

from hcf_backend.utils.hcfpal import HCFPal


logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class HCFSpiderManager(object):
    def __init__(self):
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('pid', help='Taget project id')
        parser.add_argument('spider', help='Spider name')
        parser.add_argument('frontier', help='Frontier name')
        parser.add_argument('prefix', help='Slot prefix')
        parser.add_argument('--max-jobs', help='Max number of jobs for the given spider allowed to run in parallel.\
                            Default is %(default)s.', type=int, default=1)
        parser.add_argument('--apikey',
                            help='API key to use for HCF access. Uses SH_APIKEY environment variable if not given')
        parser.add_argument('--spider-args', help='Spider arguments dict in json format', default='{}')
        parser.add_argument('--loop-mode', help='If provided, manager will run in loop mode, with a cycle each given\
                            number of seconds.', type=int, metavar='SECONDS')

        self.args = parser.parse_args()

        client = ScrapinghubClient(self.args.apikey)
        self.project = client.get_project(self.args.pid)
        self.hcfpal = HCFPal(client._hsclient.get_project(self.args.pid))

    def run(self):
        if self.args.loop_mode:
            while True:
                if not self.loop():
                    break
                time.sleep(self.args.loop_mode)
        else:
            self.loop()

    def loop(self):

        slot_re = re.compile(rf'{self.args.prefix}\d+')
        available_slots = [slot for slot in self.hcfpal.get_slots(self.args.frontier) if slot_re.match(slot)]

        running_jobs = 0
        for job in self.project.jobs.list(spider=self.args.spider, state='running', meta='spider_args'):
            frontera_settings_json = json.loads(job['spider_args'].get('frontera_settings_json', '{}'))
            if 'HCF_CONSUMER_SLOT' in frontera_settings_json:
                available_slots.remove(frontera_settings_json['HCF_CONSUMER_SLOT'])
                running_jobs += 1

        available_slots = [slot for slot in available_slots if self.hcfpal.get_slot_count(self.args.frontier, slot) > 0]
        logger.info(f"Available slots: {available_slots!r}")
        if available_slots:
            random.shuffle(available_slots)
            if self.args.max_jobs:
                max_jobs = self.args.max_jobs - running_jobs
                available_slots = available_slots[:max_jobs]
                if not available_slots:
                    logger.info(f"Already running max number of jobs.")

            for slot in available_slots:
                frontera_settings_json = json.dumps({
                    'HCF_CONSUMER_SLOT': slot
                })
                spider_args = json.loads(self.args.spider_args)
                spider_args.update({'frontera_settings_json': frontera_settings_json})
                job = self.project.jobs.run(self.args.spider, job_args=spider_args)
                logger.info(f"Scheduled job {job.key} with frontera settings {frontera_settings_json}")
            return True
        return bool(running_jobs)
