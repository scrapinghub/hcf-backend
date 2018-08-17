HCF (HubStorage Frontier) Backend for Frontera
==============================================

When used with scrapy, use it with Scrapy Scheduler provided by `scrapy-frontera <https://github.com/scrapinghub/scrapy-frontera>`_. Scrapy scheduler provided
by `Frontera <https://github.com/scrapinghub/frontera>`_ *is not* supported

See usage instructions at module and class docstrings at `backend.py <https://github.com/scrapinghub/hcf-backend/blob/master/hcf_backend/backend.py>`_.

Package also installs a convenient command line tool for hubstorage frontier handling and manipulation:
`hcfpal.py <https://github.com/scrapinghub/hcf-backend/blob/master/bin/hcfpal.py>`_. It supports dumping, count, deletion, moving, listing, etc.

Another tool provided is `hcfmanager.py <https://github.com/scrapinghub/hcf-backend/blob/master/bin/hcfmanager.py>`_. It facilitates the scheduling of
consumer spider jobs.
