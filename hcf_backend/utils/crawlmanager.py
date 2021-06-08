"""
shub-workflow based script (runs on Zyte ScrapyCloud) for easy managing of consumer spiders from multiple slots. It checks available slots and schedules
a job for each one.
"""

import json
import random
import logging

import humanize
from shub_workflow.crawl import CrawlManager

from hcf_backend.utils.hcfpal import HCFPal


logging.basicConfig(format="%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


class HCFCrawlManager(CrawlManager):

    default_max_jobs = 1

    def __init__(self):
        super().__init__()
        self.hcfpal = HCFPal(self.client._hsclient.get_project(self.project_id))

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument("frontier", help="Frontier name")
        self.argparser.add_argument("prefix", help="Slot prefix")
        self.argparser.add_argument("--frontera-settings-json")

    @property
    def description(self):
        return __doc__

    def print_frontier_status(self):
        result = self.hcfpal.get_slots_count(self.args.frontier, self.args.prefix)
        logger.info(f"Frontier '{self.args.frontier}' status:")
        for slot in sorted(result["slots"].keys()):
            cnt_text = "\t{}: {}".format(slot, result["slots"][slot])
            logger.info(cnt_text)
        logger.info("\tTotal count: {}".format(humanize.intcomma(result["total"])))

        return set(result["slots"].keys())

    def workflow_loop(self):
        available_slots = self.print_frontier_status()

        running_jobs = 0
        states = "running", "pending"
        for state in states:
            for job in self.get_project().jobs.list(
                spider=self.args.spider, state=state, meta="spider_args"
            ):
                frontera_settings_json = json.loads(
                    job["spider_args"].get("frontera_settings_json", "{}")
                )
                if "HCF_CONSUMER_SLOT" in frontera_settings_json:
                    slot = frontera_settings_json["HCF_CONSUMER_SLOT"]
                    if slot in available_slots:
                        available_slots.discard(slot)
                        running_jobs += 1

        available_slots = [
            slot
            for slot in available_slots
            if self.hcfpal.get_slot_count(self.args.frontier, slot) > 0
        ]
        logger.info(f"Available slots: {available_slots!r}")
        if available_slots:
            random.shuffle(available_slots)
            if self.max_running_jobs:
                max_jobs = self.max_running_jobs - running_jobs
                available_slots = available_slots[:max_jobs]
                if not available_slots:
                    logger.info("Already running max number of jobs.")

            base_frontera_settings = {}
            if self.args.frontera_settings_json:
                base_frontera_settings = json.loads(self.args.frontera_settings_json)
            for slot in available_slots:
                frontera_settings = base_frontera_settings.copy()
                frontera_settings.update(
                    {
                        "HCF_CONSUMER_SLOT": slot,
                        "HCF_CONSUMER_FRONTIER": self.args.frontier,
                    }
                )
                frontera_settings_json = json.dumps(frontera_settings)
                logger.info(
                    f"Will schedule spider job with frontera settings {frontera_settings_json}"
                )
                jobkey = self.schedule_spider(
                    spider_args_override={
                        "frontera_settings_json": frontera_settings_json
                    }
                )
                logger.info(
                    f"Scheduled job {jobkey} with frontera settings {frontera_settings_json}"
                )
            return True
        return bool(running_jobs)


if __name__ == "__main__":
    manager = HCFCrawlManager()
    manager.run()
