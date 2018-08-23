import pprint

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError, ConnectionError

from retrying import retry


def retry_if_http_error(e):
    for exctype in HTTPError, ConnectionError:
        if isinstance(e, exctype):
            print(f"Error: {e}. Retrying...")
            return True
    return False


class HCFPal(object):

    HCF_API_URLS = {
        'count': 'https://storage.scrapinghub.com/hcf/{pid}/{frontier}/s/{slot}/q/count',
        'list_frontiers': 'https://storage.scrapinghub.com/hcf/{pid}/list',
        'list_slots': 'https://storage.scrapinghub.com/hcf/{pid}/{frontier}/list',
    }

    def __init__(self, project):
        self.project = project

    @property
    def projectid(self):
        return self.project.projectid

    @property
    def auth(self):
        return self.project.auth

    @retry(retry_on_exception=retry_if_http_error,
           wait_fixed=60000,  # 1 min
           stop_max_attempt_number=60*24)  # 1 day
    def _get_json(self, url):
        response = requests.get(url, auth=HTTPBasicAuth(*self.auth))
        response.raise_for_status()
        return response.json()

    def get_frontiers(self):
        url = self.HCF_API_URLS['list_frontiers'].format(pid=self.projectid)
        return self._get_json(url)

    def get_slots(self, frontier):
        url = self.HCF_API_URLS['list_slots'].format(pid=self.projectid, frontier=frontier)
        return self._get_json(url)

    def delete_slots(self, frontier, slots):
        for slot in slots:
            self.project.frontier.delete_slot(frontier, slot)

    def list_all(self, prettyprint=False):
        yall = {}
        for frontier in self.get_frontiers():
            yall[frontier] = self.get_slots(frontier)
        if prettyprint:
            return pprint.pformat(yall, indent=4)
        else:
            return yall

    def get_slot_count(self, frontier, slot):
        URL = self.HCF_API_URLS['count'].format(pid=self.projectid, frontier=frontier, slot=slot)
        total = 0
        nextstart = ''
        while True:
            next_url = URL + '?start={}'.format(nextstart) if nextstart else URL
            data = self._get_json(next_url)
            total += int(data.get('count'))
            nextstart = data.get('nextstart', '')
            if not nextstart:
                break
        return total
