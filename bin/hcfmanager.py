"""
Script for easy managing of consumer spiders from multiple slots. It checks available slots and schedules a job for each one.
"""
import re
import json
import random
import logging
import argparse

from scrapinghub import ScrapinghubClient

from hcf_backend.utils.hcfpal import HCFPal


logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class Manager(object):
    def __init__(self):
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('pid', help='Taget project id')
        parser.add_argument('spider', help='Spider name')
        parser.add_argument('frontier', help='Frontier name')
        parser.add_argument('prefix', help='Slot prefix')
        parser.add_argument('--max-jobs', help='Max number of jobs for the given spider allowed to run in parallel.',
                            type=int)
        parser.add_argument('--apikey',
                            help='API key to use for HCF access. Uses SH_APIKEY environment variable if not given')

        self.args = parser.parse_args()

        client = ScrapinghubClient(self.args.apikey)
        self.project = client.get_project(self.args.pid)
        self.hcfpal = HCFPal(client._hsclient.get_project(self.args.pid))

    def run(self):

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
                job = self.project.jobs.run(self.args.spider, job_args={'frontera_settings_json': frontera_settings_json})
                logger.info(f"Scheduled job {job.key} with frontera settings {frontera_settings_json}")


if __name__ == '__main__':
    manager = Manager()
    manager.run()
