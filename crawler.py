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
    __slots__ = 'linkd_id', 'url', 'retrieved_at', \
        'raw_content', 'nofollow_links', 'links', 'title', 'text_content'

    raw_content: str            # set by LinkFetcher
    nofollow_links: list[str]   # set by LinkExtractor
    links: list[str]            # set by LinkExtractor
    title: str                  # set by TextExtractor
    text_content: str           # set by TextExtractor

    def __init__(self,
                 linkd_id: str = str(uuid.uuid4()),
                 url: str = '',
                 retrieved_at: datetime = datetime.min):
        self.linkd_id = linkd_id
        self.url = url
        self.retrieved_at = retrieved_at

    def clone(self) -> Payload:
        return deepcopy(self)

    def mark_as_processed(self):
        self.linkd_id = ''
        self.url = ''
        self.retrieved_at = datetime.min
        self.raw_content = ''
        self.nofollow_links = []
        self.links = []
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
            https://www.informit.com/articles/article.aspx?p=2359758
        """


class StageRunner(metaclass=ABCMeta):
    """ Part of stage chain, forming a pipeline """
    @abstractmethod
    def run(self, stage_params: StageParams):
        """ Implements processing logic for this stage

            - Reading incomming payloads from input, process and set output
            - Calls to run are blocked when stage input closed, or error """


class Pipeline:
    """ Modular, multi-stage pipeline
        - input source
        - output sink
        - zero or more processing stages. """

    def __init__(self, stages: list[StageRunner]):
        self.stages = stages

    def process(self, source: Source, sink: Sink):
        """ Reads contents of source, sends through stages,
            directs results to sink returning back any errors """


class Crawler(Pipeline):
    pass
