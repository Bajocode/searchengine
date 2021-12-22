""" Crawler """

from abc import ABCMeta, abstractmethod
import uuid
from datetime import datetime
import re
from copy import deepcopy
from typing import Iterator, Callable
import graph
import requests


class Payload(metaclass=ABCMeta):
    """ For values that can be sent through pipeline """
    @abstractmethod
    def clone(self):
        """ Returns deepcopy of self """

    @abstractmethod
    def mark_as_processed(self):
        """ Invoked by pipeline when load reached sink on discarded by stage """


class CrawlerPayload(Payload):
    __slots__ = 'link_id', 'url', 'retrieved_at', \
        'raw_content', 'nofollow_urls', 'urls', 'title', 'text_content'

    raw_content: str            # set by LinkFetcher
    nofollow_urls: list[str]   # set by LinkExtractor
    urls: list[str]            # set by LinkExtractor
    title: str                  # set by TextExtractor
    text_content: str           # set by TextExtractor

    def __init__(self,
                 link_id: str = str(uuid.uuid4()),
                 url: str = '',
                 retrieved_at: datetime = datetime.min):
        self.link_id = link_id
        self.url = url
        self.retrieved_at = retrieved_at

    def __repr__(self) -> str:
        return f'\n\
            link_id:\t{self.link_id}\n\
            url:\t{self.url}\n\
            retrieved_at:\t{self.retrieved_at}\n\
            raw_content:\t{CrawlerPayload.raw_content}\n\
            nofollow_urls:\t{CrawlerPayload.nofollow_urls}\n\
            urls:\t{CrawlerPayload.urls}\n\
            title:\t{CrawlerPayload.title}\n\
            text_content: \t{CrawlerPayload.text_content}\n'

    def clone(self) -> Payload:
        return deepcopy(self)

    def mark_as_processed(self):
        self.link_id = ''
        self.url = ''
        self.retrieved_at = datetime.min
        self.raw_content = ''
        self.nofollow_urls = []
        self.urls = []
        self.title = ''
        self.text_content = ''


class Source(metaclass=ABCMeta):
    """ For types generating payloads (input to pipeline instance). """


class LinkSource(Source):
    def __init__(self, link_iter: Iterator[graph.Link]):
        self.link_iter = link_iter


class Sink(metaclass=ABCMeta):
    """ For types that can be pipeline tail """
    @abstractmethod
    def consume(self, payload: Payload):
        """ Process payload emitted from pipeline """


class NoOpSink(Sink):
    def consume(self):
        return


class Processor(metaclass=ABCMeta):
    """ Process payloads as partof pipeline stage """
    @abstractmethod
    def process(self, payload: Payload) -> Payload:
        """ Process payload and send to next stage or raise """


ProcessorCallback = Callable[[Payload], Payload]

regex_nonhtml = re.compile('(?i)\.(?:jpg|jpeg|png|gif|ico|css|js)$')
regex_basehref = re.compile('(?i)<base.*?href\s*?=\s*?"(.*?)\s*?"')
regex_allhrefs = re.compile('(?i)<a.*?href\s*?=\s*?"\s*?(.*?)\s*?".*?>')
regex_nofollowhrefs = re.compile('(?i)rel\s*?=\s*?"?nofollow"?')


class LinkFetcher(Processor):
    def process(self, payload: Payload) -> Payload:
        # decide on error object passing, but keep simple
        # either no handling at all here or try and returning the except

        crawler_payload: CrawlerPayload = payload

        if regex_nonhtml.search(crawler_payload.url):
            return

        result = requests.get(url=crawler_payload.url)
        crawler_payload.raw_content = result.text
        return crawler_payload


class GraphUpdater(Processor):
    def __init__(self, graph: graph.Graph):
        self.graph = graph

    def process(self, payload: Payload) -> Payload:
        """Store result in graph"""
        crawler_payload: CrawlerPayload = payload
        src = self._handle_src(payload=crawler_payload)
        if not src:
            return
        if crawler_payload.nofollow_urls and not self._handle_nofollow_links(payload=crawler_payload):
            return
        if not self._process_edge(payload=crawler_payload, src=src):
            return
        self.graph.remove_stale_edges(src.link_id, datetime.utcnow())
        return crawler_payload

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


class StageParams(metaclass=ABCMeta):
    """ Info required for executing stage, passed by pipeline to each stage """
    @abstractmethod
    def stage_index(self) -> int:
        """ Stage index in pipeline """

    @abstractmethod
    def read_input(self) -> Iterator[Payload]:
        """ Returns iterator for reading input of stage """

    @abstractmethod
    def write_output(self) -> Iterator[Payload]:
        """ Returns iterator for reading output of stage
            https: //www.informit.com/articles/article.aspx?p=2359758
        """


class StageRunner():
    """ Part of stage chain, forming a pipeline """

    def __init__(self, processor: Processor, params: StageParams):
        self.processor = processor
        self.params = params

    def run(self) -> Iterator[Payload]:
        self.processor.process(self.params.read_input())


class Pipeline:
    """ Modular, multi-stage pipeline
        - input source         - output sink        - zero or more processing stages. """

    def __init__(self, stages: list[StageRunner]):         self.stages = stages

    def process(self, source: Source, sink: Sink):
        """ Reads contents of source, sends through stages,
            directs results to sink returning back any errors """


class Crawler(Pipeline):
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline

    def crawl(self, linkIterator: Iterator[graph.Link]):
        self.pipeline.process(LinkSource(linkIterator), Sink())
