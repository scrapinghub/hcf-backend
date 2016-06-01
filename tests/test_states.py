# -*- coding: utf-8 -*-
import config
from hcf_backend.manager import HCFStates
from frontera.core.models import Request
from random import randint, choice
from sys import maxint
import logging


def generate_fprint():
    return ''.join(map(lambda x:choice('0123456789abcdef'), range(40)))


def check_states(states, fprints, objs):
    states.fetch(fprints)
    objs_fresh = [Request(o.url, meta={'fingerprint': o.meta['fingerprint']}) for o in objs]
    states.set_states(objs_fresh)
    i1 = iter(objs)
    i2 = iter(objs_fresh)

    while True:
        try:
            o1 = i1.next()
            o2 = i2.next()
            assert o1.meta['fingerprint'] == o2.meta['fingerprint']
            assert o1.meta['state'] == o2.meta['state']
        except StopIteration:
            break

def test_states():
    logging.basicConfig(level=logging.DEBUG)
    states = HCFStates(config.API_KEY, config.PROJECT_ID, config.FRONTIER_NAME, 256, True)
    states.frontier_start()
    objs = []
    fprints = []
    for i in range(0, 128):
        o = Request('http://website.com/%d' % randint(0, maxint))
        o.meta['fingerprint'] = generate_fprint()
        o.meta['state'] = choice([HCFStates.NOT_CRAWLED, HCFStates.QUEUED, HCFStates.CRAWLED, HCFStates.ERROR])
        objs.append(o)
        fprints.append(o.meta['fingerprint'])

    states.update_cache(objs)
    states.flush()

    # cache is warm
    check_states(states, fprints, objs)

    # clearing tha cache, and testing fetching
    states.flush(force_clear=True)
    check_states(states, fprints, objs)





