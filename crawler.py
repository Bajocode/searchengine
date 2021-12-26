""" Crawler """

from abc import ABCMeta, abstractmethod
import uuid
from datetime import datetime
import re
from copy import deepcopy
from typing import Iterator, Callable
import graph
import requests
from urllib.parse import urlparse


class CrawlerPayload():
    __slots__ = 'link_id', 'url', 'retrieved_at', \
        'raw_content', 'nofollow_urls', 'urls', 'title', 'text_content'

    def __init__(self,
                 link_id: str = str(uuid.uuid4()),
                 url: str = '',
                 retrieved_at: datetime = datetime.min,
                 raw_content: str = '',                  # set by LinkFetcher
                 nofollow_urls: list[str] = [],          # set by LinkExtractor
                 urls: list[str] = [],                   # set by LinkExtractor
                 title: str = '',                        # set by TextExtractor
                 text_content: str = ''):                # set by TextExtractor
        self.link_id = link_id
        self.url = url
        self.retrieved_at = retrieved_at
        self.raw_content = raw_content
        self.nofollow_urls = nofollow_urls
        self.urls = urls
        self.title = title
        self.text_content = text_content

    def __repr__(self) -> str:
        return f'\n\
            link_id:\t{self.link_id}\n\
            url:\t{self.url}\n\
            retrieved_at:\t{self.retrieved_at}\n\
            raw_content:\t{self.raw_content}\n\
            nofollow_urls:\t{self.nofollow_urls}\n\
            urls:\t{self.urls}\n\
            title:\t{self.title}\n\
            text_content: \t{self.text_content}\n'


class Processor(metaclass=ABCMeta):
    """ Process payloads as partof pipeline stage """
    @ abstractmethod
    def process(self, payload: CrawlerPayload) -> CrawlerPayload:
        """ Process payload and send to next stage or raise """


class LinkFetcher(Processor):
    def process(self, payload: CrawlerPayload) -> CrawlerPayload:
        # decide on error object passing, but keep simple
        # either no handling at all here or try and returning the except

        crawler_payload: CrawlerPayload = payload

        if regex_nonhtml.search(crawler_payload.url):
            return

        result = requests.get(url=crawler_payload.url)
        crawler_payload.raw_content = result.text
        return crawler_payload


regex_nonhtml = re.compile('(?i)\.(?:jpg|jpeg|png|gif|ico|css|js)$')
regex_basehref = re.compile('(?i)<base.*?href\s*?=\s*?"(.*?)\s*?"')
regex_allhrefs = re.compile('(?i)<a.*?href\s*?=\s*?"\s*?(.*?)\s*?".*?>')
regex_nofollowhrefs = re.compile('(?i)rel\s*?=\s*?"?nofollow"?')


class LinkExtractor(Processor):
    def process(self, payload: CrawlerPayload) -> CrawlerPayload:
        allhrefs = regex_allhrefs.findall(payload.raw_content)
        payload.urls = [url for url in allhrefs if self._is_absolute(url)]
        return payload

    def _is_absolute(self, urlstring: str) -> bool:
        parsed = urlparse(urlstring)
        return parsed.scheme and parsed.netloc


class GraphUpdater(Processor):
    def __init__(self, graph: graph.Graph):
        self.graph = graph

    def process(self, payload: CrawlerPayload) -> CrawlerPayload:
        """Store result in graph"""
        src = self._handle_src(payload=payload)
        removal_treshold = datetime.utcnow()

        if not src:
            return
        if payload.nofollow_urls and not self._handle_nofollow_links(payload=payload):
            return
        if not self._process_edge(payload=payload, src=src):
            return
        self.graph.remove_stale_edges(src.link_id, removal_treshold)
        return payload

    def _handle_src(self, payload: CrawlerPayload):
        link_src = graph.Link(
            link_id=payload.link_id,
            url=payload.url,
            retrieved_at=datetime.utcnow()
        )
        return self.graph.upsert_link(link_src)

    def _handle_nofollow_links(self, payload: CrawlerPayload):
        """Upsert discovered no-follow links without creating an edge."""
        upserted = []
        for nofollow_url in payload.nofollow_urls:
            link_dst = graph.Link(url=nofollow_url)
            upserted.append(self.graph.upsert_link(link_dst))
        return upserted

    def _process_edge(self, payload: CrawlerPayload, src: graph.Link):
        edges = []
        for dst_url in payload.urls:
            dst = self.graph.upsert_link(link=graph.Link(url=dst_url))
            if not dst:
                return
            edge = graph.Edge(src=src.link_id, dst=dst.link_id)
            edges.append(self.graph.upsert_edge(edge=edge))
        return edges


class Crawler():
    def __init__(self, graph: graph.Graph):
        self.stages = [
            LinkFetcher(),
            LinkExtractor(),
            GraphUpdater(graph=graph)
        ]

    def run(self, links_iter: Iterator[graph.Link]):
        outputs = []
        for link in links_iter:
            payload_in = CrawlerPayload(link_id=link.link_id,
                                        url=link.url,
                                        retrieved_at=link.retrieved_at)
            payload_out = self.crawl(payload_in)
            outputs.append(payload_out)
        return outputs

    def crawl(self, payload: CrawlerPayload) -> CrawlerPayload:
        inout = payload
        for stage in self.stages:
            inout = stage.process(inout)
        return inout
