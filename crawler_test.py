"""CrawlerTestCase"""

import unittest
import indexer
import uuid
from datetime import datetime
import crawler
import graph


class LinkFetcherTestCase(unittest.TestCase):
    """LinkFetcherTestCase """

    def setUp(self):
        self.sut = crawler.LinkFetcher()

    def test_fetch_correctlink_successfully(self):
        payload_in = crawler.CrawlerPayload(url="http://google.com")
        payload_out = self.sut.process(payload_in)

        self.assertIsNotNone(payload_out)

    def test_fetch_with_nonhtml_extension(self):
        payload_in = crawler.CrawlerPayload(url="http://google.com/a.png")
        payload_out = self.sut.process(payload_in)

        self.assertIsNone(payload_out)


class GraphUpdaterTestCase(unittest.TestCase):
    """GraphUpdaterTestCase """

    def setUp(self):
        self.sut = crawler.GraphUpdater(graph=graph.GraphInMemory())

    def test_fetch_correctlink_successfully(self):
        payload = crawler.CrawlerPayload(
            link_id=uuid.uuid4(),
            url='http://example.com',
        )
        payload.nofollow_urls = ['http://forum.com']
        payload.urls = ['http://example.com/foo', 'http://example.com/bar']
        payload = self.sut.process(payload=payload)
        print(payload)


if __name__ == '__main__':
    unittest.main()
