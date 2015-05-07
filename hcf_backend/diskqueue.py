import os
from os.path import join, exists
from queuelib import PriorityQueue

from frontera.utils.misc import load_object

class DiskQueue(object):
    def __init__(self, dqclasspath, request_model):
        self.dqclass = load_object(dqclasspath)
        self.request_model = request_model
        self.dq = PriorityQueue(self._dqfactory)
        self.__func_ids = {}

    def get_func_id(self, func):
        objid = id(func)
        self.__func_ids.setdefault(objid, func)
        return objid

    def get_function_from_id(self, fid):
        return self.__func_ids[fid]

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

    def request_to_dict(self, request):
        rdict = {}
        for attr in ('url', 'method', 'headers', 'cookies', 'meta'):
            rdict[attr] = getattr(request, attr)
        if 'scrapy_callback' in rdict['meta']:
            func = rdict['meta'].pop('scrapy_callback')
            rdict['meta']['scrapy_callback_id'] = self.get_func_id(func)
        return rdict

    def request_from_dict(self, rdict):
        fid = rdict['meta'].pop('scrapy_callback_id')
        rdict['meta']['scrapy_callback'] = self.get_function_from_id(fid)
        return self.request_model(**rdict)

    def __len__(self):
        return len(self.dq)
