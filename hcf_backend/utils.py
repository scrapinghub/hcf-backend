
class ScrapyStatsCollectorWrapper(object):
    """
    Scrapy StatsCollector wrapper class.
    Calls StatsCollector methods if its initialized with one. If not, use dummy methods.
    """
    def __init__(self, stats):
        self._stats = stats

    def __getattr__(self, name):
        def dummy_method(*args, **kwargs):
            pass
        stats = self.__dict__.get('_stats', None)
        if stats:
            return getattr(stats, name)
        else:
            return dummy_method


def get_scrapy_crawler(data):
    return data.get('crawler', None)


def get_scrapy_stats(data):
    crawler = get_scrapy_crawler(data)
    return crawler.stats if crawler else None
