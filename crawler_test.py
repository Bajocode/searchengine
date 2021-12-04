"""CrawlerTestCase"""

import unittest
import indexer
import uuid
from datetime import datetime
import crawler


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


if __name__ == '__main__':
    unittest.main()
