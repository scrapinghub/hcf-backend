import json


class HCFSlot(object):

    def __init__(self, client, frontier, slot, batch_size=100):
        self.client = client
        self.frontier = frontier
        self.slot = slot
        self.batch_size = batch_size
        self._writer = self._get_writer()
        self.stats = {'newcount': 0}

    def _get_writer(self):
        writer = self.client._get_writer(self.frontier, self.slot)
        writer.callback = self._inc_newcount
        writer.size = self.batch_size
        return writer

    def _inc_newcount(self, response):
        self.stats['newcount'] += json.loads(response.content)['newcount']

    def write(self, data):
        self.client.add(self.frontier, self.slot, data)

    def write_one(self, data):
        self.write([data])

    def read(self, count=100):
        return list(self.client.read(self.frontier, self.slot, mincount=count))

    def read_one(self):
        return (self.read(count=1) or [None])[0]

    def flush(self):
        self._writer.flush()

    def delete(self, ids):
        self.client.delete(self.frontier, self.slot, ids)

    def truncate(self):
        self.client.delete_slot(self.frontier, self.slot)

    def close(self):
        self._writer.close(block=True)
