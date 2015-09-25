import unittest

from hcf_backend.manager import HCFManager


class HCFManagerTest(unittest.TestCase):
    auth = 'ffffffffffffffffffffffffffffffff'
    endpoint = 'http://storage.dev.scrapinghub.com:8002'
    project_id = '222222222'
    slots = (('test', 0), ('test', 1),
            ('test1', 0), ('test1', 1), ('test1', 2))

    def setUp(self):
        self.manager = HCFManager(self.auth, self.project_id,
                endpoint=self.endpoint)

    def tearDown(self):
        try:
            for frontier, slot in self.manager._hcf_slots.keys():
                self.manager.delete_slot(frontier, slot)
                if self.manager.read(frontier, slot, mincount=1):
                    raise Exception('Something went wrong')
        finally:
            self.manager.close()

    def test_create_slot(self):
        slot_info = self.slots[0]
        slot = self.manager.create_slot_if_needed(*slot_info)
        self.assertIs(self.manager.create_slot_if_needed(*slot_info), slot)

    def test_add_request_read(self):
        data = {'fp': '0', 'qdata': {}}
        for frontier, slot in self.slots:
            self.manager.add_request(frontier, slot, data)
            self.manager.flush(frontier, slot)
            batch = self.manager.read(frontier, slot, mincount=1)[0]
            self.assertEqual(batch['requests'][0][0], data['fp'])

    def test_delete_slot(self):
        data = {'fp': '0', 'qdata': {}}
        for frontier, slot in self.slots:
            self.manager.add_request(frontier, slot, data)
            self.manager.flush(frontier, slot)
            batch = self.manager.read(frontier, slot, mincount=1)[0]
            self.assertEqual(batch['requests'][0][0], data['fp'])
        self.manager.delete_slot(frontier, slot)
        batch = self.manager.read(frontier, slot, mincount=1)
        self.assertEquals(batch, [])


if __name__ == '__main__':
    unittest.main()
