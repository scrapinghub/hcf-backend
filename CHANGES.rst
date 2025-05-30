=====================
hcf-backend changelog
=====================

0.6.0 (unreleased)
==================

-   Dropped Python 3.6-3.8 support, added Python 3.9+ support.

-   | Updated dependencies:
    | ``frontera``: ``==0.7.1`` → ``>=0.7.2,<8``
    | ``humanize``: ``==0.5.1`` → ``>=0.5.1``
    | ``scrapinghub``: ``>=2.0.0`` → ``>=2.3.1``

-   Added support for binary fingerprints.

    This makes it possible to work with Scrapy 2.7+ through scrapy-frontera.

-   ``HCFBackend.hcf_get_producer_slot()`` must now accept an ``fp`` parameter.


Earlier releases
================

Find the earlier commit history `at GitHub
<https://github.com/scrapinghub/hcf-backend/commits/87ad29b650637b93c5935b096d31d1f8b209fab9/>`_.
