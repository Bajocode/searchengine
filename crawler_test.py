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


# class GraphUpdaterTestCase(unittest.TestCase):
#     """GraphUpdaterTestCase """

#     def setUp(self):
#         self.sut = crawler.GraphUpdater(graph=graph.GraphInMemory())

#     def test_fetch_correctlink_successfully(self):
#         payload = crawler.CrawlerPayload(
#             link_id=uuid.uuid4(),
#             url='http://example.com',
#         )
#         payload.nofollow_urls = ['http://forum.com']
#         payload.urls = ['http://example.com/foo', 'http://example.com/bar']
#         payload = self.sut.process(payload=payload)


class CrawlerTestCase(unittest.TestCase):
    def setUp(self):
        self.graph = graph.GraphInMemory()
        self.sut = crawler.Crawler(graph=self.graph)

    def test_crawls_successfully(self):
        link_local = graph.Link(url='https://en.wikipedia.org')
        self.graph.upsert_link(link=link_local)
        links_iter = self.graph.links_iter(
            uuid.UUID(int=0),
            uuid.UUID('{FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF}'),
            retrieved_before=datetime.utcnow())
        outputs = self.sut.run(links_iter)

        print(self.graph.edges)


if __name__ == '__main__':
    unittest.main()
