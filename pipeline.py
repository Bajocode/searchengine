""" Pipeline. """

from abc import ABCMeta, abstractmethod
from typing import Iterator


class Payload(metaclass=ABCMeta):
    """ For values that can be sent through pipeline """
    @abstractmethod
    def clone(self):
        """ Returns deepcopy of self """

    @abstractmethod
    def mark_completed(self):
        """ Invoked by pipeline when load reached sink on discarded by stage """


class Processor(metaclass=ABCMeta):
    """ Process payloads as partof pipeline stage """
    @abstractmethod
    def process(self, payload: Payload) -> Payload:
        """ Process payload and send to next stage or raise """


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


class Source(metaclass=ABCMeta):
    """ For types generating payloads (input to pipeline instance). """
    @abstractmethod
    def next(self) -> bool:
        """ Fetch next payload from source, if none left reurn False """

    @abstractmethod
    def next_payload(self) -> Payload:
        """ Returns next payload to be processed """

    @abstractmethod
    def last_error(self):
        """ Returns last error observerd by source """


class Sink(metaclass=ABCMeta):
    """ For types that can be pipeline tail """
    @abstractmethod
    def consume(self, payload: Payload):
        """ Process payload emitted from pipeline """
