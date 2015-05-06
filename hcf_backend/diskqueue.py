import os
from os.path import join, exists
from queuelib import PriorityQueue

from frontera.utils.misc import load_object

class DiskQueue(object):
    def __init__(self, dqclasspath, request_model):
        self.dqclass = load_object(dqclasspath)
        self.request_model = request_model
        self.dq = PriorityQueue(self._dqfactory)

    def _dqfactory(self, priority):
        return self.dqclass(join(self._get_dqdir(), 'p%s' % priority))

    @staticmethod
    def _get_dqdir():
        dqdir = 'requests.queue'
        if not exists(dqdir):
            os.makedirs(dqdir)
        return dqdir

    def push(self, request):
        rdict = self.request_to_dict(request)
        self.dq.push(rdict)

    def pop(self):
        rdict = self.dq.pop()
        if rdict:
            return self.request_from_dict(rdict)

    @staticmethod
    def request_to_dict(request):
        rdict = {}
        for attr in ('url', 'method', 'headers', 'cookies', 'meta'):
            rdict[attr] = getattr(request, attr)
        if 'scrapy_callback' in rdict['meta']:
            func = rdict['meta']['scrapy_callback']
            rdict['meta']['scrapy_callback'] = func.im_func.__name__ 
        return rdict

    def request_from_dict(self, rdict):
        request = self.request_model(**rdict)
        return request

    def __len__(self):
        return len(self.dq)
