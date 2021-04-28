# HCF (HubStorage Crawl Frontier) Backend for Frontera

When used with scrapy, use it with Scrapy Scheduler provided by [scrapy-frontera](https://github.com/scrapinghub/scrapy-frontera). Scrapy scheduler provided
by [Frontera](https://github.com/scrapinghub/frontera) *is not* supported. `scrapy-frontera` is a scrapy scheduler which allows to use frontera backends,
like the present one, with scrapy projects.

See specific usage instructions at module and class docstrings at [backend.py](https://github.com/scrapinghub/hcf-backend/blob/master/hcf_backend/backend.py).
Some examples of usage can be seen in the [scrapy-frontera README](https://github.com/scrapinghub/scrapy-frontera/blob/master/README.rst).

A complete tutorial for using `hcf-backend` with ScrapyCloud workflows is available at
[shub-workflow Tutorial: Managing Hubstorage Crawl Frontiers](https://github.com/scrapinghub/shub-workflow/wiki/Managing-Hubstorage-Crawl-Frontiers). `shub-workflow` is a framework for
defining workflows of spiders and scripts running over ScrapyCloud. This is a strongly recommended lecture, because it documents the integration of different tools
which together provide the best benefit.

Package also provides a convenient command line tool for hubstorage frontier handling and manipulation:
[hcfpal.py](https://github.com/scrapinghub/hcf-backend/blob/master/hcf_backend/utils/hcfpal.py). It supports dumping, count, deletion, moving, listing, etc.
See command line help for usage.

Another provided tool is [crawlmanager.py](https://github.com/scrapinghub/hcf-backend/blob/master/hcf_backend/utils/crawlmanager.py). It facilitates the
scheduling of consumer spider jobs. Examples of usage are also available in the already mentioned `shub-workflow` Tutorial.

Installation
============

`pip install hcf-backend`
