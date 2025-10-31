=====================
hcf-backend changelog
=====================

0.6.1 (2025-10-31)
==================

- fix: Lowered log level for retry messages from error to warning. Errors are now only logged for exhausted retries.
- chore: Added configuration support for bump-my-version.

0.6.0 (2025-06-06)
==================

-   Dropped Python 3.6-3.8 support, added Python 3.9+ support.

-   | Updated dependencies:
    | ``frontera``: ``==0.7.1`` → ``>=0.7.2,<8``
    | ``humanize``: ``==0.5.1`` → ``>=0.5.1``
    | ``scrapinghub``: ``>=2.0.0`` → ``>=2.3.1``

-   A non-str request fingerprint will now raise a more descriptive
    :exc:`ValueError` exception.


Earlier releases
================

Find the earlier commit history `at GitHub
<https://github.com/scrapinghub/hcf-backend/commits/87ad29b650637b93c5935b096d31d1f8b209fab9/>`_.
