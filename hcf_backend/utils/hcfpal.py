import re
import pprint
from itertools import cycle, groupby
from operator import itemgetter

import humanize
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError, ConnectionError
from retrying import retry
from shub_workflow.script import BaseScript

from hcf_backend.utils import assign_slotno


def retry_if_http_error(e):
    for exctype in HTTPError, ConnectionError:
        if isinstance(e, exctype):
            print(f"Error: {e}. Retrying...")
            return True
    return False


class HCFPal:

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

    def get_slots_count(self, frontier, prefix, num_slots=None, regex=''):
        result = {'slots': {}}
        total = 0
        not_empty_slots = 0
        slots = ['{}{}'.format(prefix, slot) for slot in range(num_slots)] if \
            num_slots else self.get_slots(frontier)
        for slot in slots:
            if not slot.startswith(prefix):
                continue
            if not re.search(regex, slot):
                continue
            cnt = self.get_slot_count(frontier, slot)
            if cnt:
                not_empty_slots += 1
            total += cnt
            result["slots"][slot] = cnt
        result['total'] = total
        result['not empty slots'] = not_empty_slots
        return result

    def dump_slot(self, frontier, slot, max_requests):
        count = 0
        for batch in self.project.frontier.read(frontier, slot, max_requests):
            for request in batch['requests']:
                yield batch["id"], request
                count += 1
                if count == max_requests:
                    return


# TODO: move code from this script to HCFPal class, leaving here only command line support
class HCFPalScript(BaseScript):

    flow_id_required = False

    def __init__(self):
        super().__init__()
        hsc = self.client._hsclient
        self.hsp = hsc.get_project(self.project_id)
        self.hcf = HCFPal(self.hsp)

    @property
    def description(self):
        return 'Helper script for accessing HubCrawlFrontier.'

    def add_argparser_options(self):
        super().add_argparser_options()
        subparsers = self.argparser.add_subparsers(dest='cmd')
        parser_list = subparsers.add_parser('list', help='List project frontiers or slots in a frontier')
        parser_list.add_argument('frontier', nargs='?', help='Define frontier to list it\'s slots')
        parser_list.add_argument('--all', action='store_true', help='List all frontiers and their slots')

        parser_count = subparsers.add_parser('count', help='Count requests in frontier slots')
        parser_count.add_argument('frontier', help='Frontier for which to count')
        parser_count.add_argument('--prefix', help='Count only slots with a given prefix', default='')
        parser_count.add_argument('--regex', help='Count only slots that matches given regex', default='')
        parser_count.add_argument('--num-slots', type=int, help="Specify number of slots instead of autodetect \
                                                                 (much faster in most cases)")

        parser_delete = subparsers.add_parser('delete', help='Delete slots from frontier')
        parser_delete.add_argument('frontier', help='Frontier to delete slots from')
        parser_delete.add_argument('prefix', help='Delete only slots with a given prefix')

        parser_dump = subparsers.add_parser('dump', help='Dump next requests in queue of a frontier slot')
        parser_dump.add_argument('frontier', help='Frontier name from where to dump')
        parser_dump.add_argument('slot', help='Slot from where to dump')
        parser_dump.add_argument('--num-requests', help='Number of requests to dump. Defaults to %(default)d.',
                                 type=int, default=100)

        parser_move = subparsers.add_parser('move', help='Move requests from slots of given prefix, into the given \
                                                          number of slots on another prefix.')
        parser_move.add_argument('frontier', help='Frontier name')
        parser_move.add_argument('prefix', help='Prefix name of the source slots')
        parser_move.add_argument('dest_prefix', help='Prefix name of the destination slots')
        parser_move.add_argument('dest_num_slots', help='Number of destination slots', type=int)
        parser_move.add_argument('--num-slots', type=int, help='If given, source slots are computed using given prefix \
                                                                and this number instead of list api (sometimes list \
                                                                api works very slow)')
        parser_move.add_argument('--uniform', action='store_true',
                                 help='Distribute requests uniformly among slots. By default uses standard \
                                       assignation mapping.')

        parser_move_batch = subparsers.add_parser('move_batch',
                                                  help='Move requests from given batch id into a new slot.')
        parser_move_batch.add_argument('frontier', help='Frontier name')
        parser_move_batch.add_argument('source_slot', help='Source slot where to find the batch id')
        parser_move_batch.add_argument('batchid', help='Id of the target batch')
        parser_move_batch.add_argument('dest_slot', help='Destination slot')
        parser_move_batch.add_argument('--max-scan-batches', default=100, type=int,
                                       help='Max number of batches to scan in order to find target batch id in the \
                                             source slot')

    def run(self):
        if self.args.cmd == 'list':
            self.list_hcf()
        elif self.args.cmd == 'count':
            self.count_slots()
        elif self.args.cmd == 'delete':
            self.delete_slots()
        elif self.args.cmd == 'dump':
            self.dump_slot()
        elif self.args.cmd == 'move':
            self.move_slots()
        elif self.args.cmd == 'move_batch':
            self.move_batch()
        else:
            self.argparser.print_help()

    def delete_slots(self):
        prefix_note = ' (with prefix "{}")'.format(self.args.prefix) if self.args.prefix else ''
        print('Deleting slots{} from frontier "{}", project {}...'.format(
            prefix_note, self.args.frontier, self.project_id))
        slots = [slot for slot in self.hcf.get_slots(self.args.frontier) if slot.startswith(self.args.prefix)]
        self.hcf.delete_slots(self.args.frontier, slots)
        print('Slots deleted: {}'.format(slots))

    def list_hcf(self):
        if self.args.all:
            print('Listing all frontiers and their slots in project {}:'.format(self.project_id))
            print(self.hcf.list_all(prettyprint=True))
        elif self.args.frontier:
            print('Listing slots for frontier "{}" in project {}:'.format(self.args.frontier, self.project_id))
            for slot in self.hcf.get_slots(self.args.frontier):
                print('\t{}'.format(slot))
        else:
            print('Listing frontiers in project {}:'.format(self.project_id))
            for front in self.hcf.get_frontiers():
                print('\t{}'.format(front))

    def count_slots(self):
        note = ''
        if self.args.prefix:
            note = ' (with prefix "{}")'.format(self.args.prefix)
        elif self.args.regex:
            note = ' (with regex "{}")'.format(self.args.regex)
        print('Counting requests in slots{} for frontier "{}", project {}:'.format(
            note, self.args.frontier, self.project_id))
        result = self.hcf.get_slots_count(self.args.frontier, self.args.prefix, self.args.num_slots, self.args.regex)
        for slot in sorted(result['slots'].keys()):
            cnt_text = '\t{}: {}'.format(slot, result['slots'][slot])
            print(cnt_text)
        print('\t' + '-' * 25)
        print('\tTotal count: {}'.format(humanize.intcomma(result['total'])))
        print('\tNot-empty slots: {}'.format(result['not empty slots']))

    def dump_slot(self):
        print('Dumping next {} requests from slot {}, frontier {}, pid {}:'.format(
            self.args.num_requests, self.args.slot, self.args.frontier, self.project_id))
        for batch_id, reqs in groupby(self.hcf.dump_slot(self.args.frontier, self.args.slot, self.args.num_requests), key=itemgetter(0)):
            print("Batch id:", batch_id)
            for _, req in reqs:
                print(req)

    def move_slots(self):
        print("Moving requests from frontier {}, pid {}, prefix {} into {} slots of prefix {}".format(
            self.args.frontier, self.project_id, self.args.prefix, self.args.dest_num_slots,
            self.args.dest_prefix))
        if self.args.num_slots:
            source_slots = [self.args.prefix + str(slotno) for slotno in range(self.args.num_slots)]
        else:
            # use list api
            source_slots = [slot for slot in self.hcf.get_slots(self.args.frontier) if
                            slot.startswith(self.args.prefix)]
        cyclic_gen = cycle(range(self.args.dest_num_slots))
        for slot in source_slots:
            print("Reading slot %s" % slot)
            while True:
                # read each one batch
                for batch in self.hsp.frontier.read(self.args.frontier, slot, 1):
                    for fp, qdata in batch['requests']:
                        if self.args.uniform:
                            dslotno = next(cyclic_gen)
                        else:
                            dslotno = assign_slotno(fp, self.args.dest_num_slots)
                        dslot = self.args.dest_prefix + str(dslotno)
                        self.hsp.frontier.add(self.args.frontier, dslot, [{'fp': fp, 'qdata': qdata}])
                    # we don't want to generate batches bigger than source ones
                    self.hsp.frontier.flush()
                    count = len(batch['requests'])
                    self.hsp.frontier.delete(self.args.frontier, slot, [batch['id']])
                    print("Moved batch %s (%d requests) from slot %s" % (batch['id'], count, slot))
                    break
                else:
                    break
            self.hcf.delete_slots(self.args.frontier, [slot])

    def move_batch(self):
        print("Moving requests from frontier {}, pid {}, slot {}, batch {} to slot {}".format(
            self.args.frontier, self.project_id, self.args.source_slot, self.args.batchid, self.args.dest_slot))
        for batch in self.hsp.frontier.read(self.args.frontier, self.args.source_slot, self.args.max_scan_batches):
            if batch['id'] == self.args.batchid:
                frequests = []
                for fp, qdata in batch['requests']:
                    frequests.append({'fp': fp, 'qdata': qdata})
                self.hsp.frontier.add(self.args.frontier, self.args.dest_slot, frequests)
                self.hsp.frontier.delete(self.args.frontier, self.args.source_slot, [batch['id']])
                self.hsp.frontier.flush()
                break


if __name__ == '__main__':
    script = HCFPalScript()
    script.run()
