import unittest

from hubstorage import HubstorageClient

from hcf_backend.slot import HCFSlot


class HCFSlotTest(unittest.TestCase):
    auth = 'ffffffffffffffffffffffffffffffff'
    endpoint = 'http://storage.dev.scrapinghub.com:8002'
    project_id = '222222222'
    frontier = 'test'
    slot = '0'

    def setUp(self):
        hsc = HubstorageClient(auth=self.auth, endpoint=self.endpoint)
        hsp = hsc.get_project(self.project_id)
        self.slot = HCFSlot(hsp.frontier, self.frontier, self.slot)

    def tearDown(self):
        self.slot.truncate()
        try:
            if self.slot.read_one():
                raise Exception('Something went wrong')
        finally:
            self.slot.close()

    def test_write_read(self):
        data = {'fp': '0', 'qdata': {}}
        self.slot.write_one(data)
        self.slot.flush()
        batch = self.slot.read_one()
        self.assertEqual(batch['requests'][0][0], data['fp'])

    def test_newcount(self):
        data = {'fp': '0', 'qdata': {}}
        self.slot.write_one(data)
        self.slot.flush()
        self.assertEqual(self.slot.stats['newcount'], 1)


if __name__ == '__main__':
    unittest.main()
