"""A one line summary of the module or program, terminated by a period.

Leave one blank line.  The rest of this docstring should contain an
overall description of the module or program.  Optionally, it may also
contain a brief description of exported classes and functions and/or usage
examples.

  Typical usage example:

  foo = ClassFoo()
  bar = foo.FunctionBar()
"""

from abc import ABCMeta, abstractmethod
from enum import Enum
import common
from datetime import datetime
from copy import deepcopy
from typing import Iterator


class Link:
    """ Encapsulates all information about a link discovered by crawler """
    __slots__ = 'link_id', 'url', 'retrieved_at'

    def __init__(self,
                 link_id: common.UUID = str(uuid.UUID(int=0)),
                 url: str = '',
                 retrieved_at: common.Timestamp = 0):
        self.link_id = link_id
        self.url = url
        self.retrieved_at = retrieved_at

    def __eq__(self, other):
        return self.link_id == other.link_id \
            and self.url == other.url \
            and self.retrieved_at == other.retrieved_at

    def __deepcopy__(self, memo):  # memo is a dict of id's to copies
        id_self = id(self)        # memoization avoids unnecesary recursion
        copy_self = memo.get(id_self, False)
        if copy_self:
            return copy_self
        copy_self = type(self)(
            deepcopy(self.link_id, memo),
            deepcopy(self.url, memo),
            deepcopy(self.retrieved_at, memo))
        memo[id_self] = copy_self
        return copy_self

    def __repr__(self) -> str:
        return f'\n\
            link_id:\t{self.link_id}\n\
            url:\t{self.url}\n\
            retrieved_at:\t{self.retrieved_at}\n'


class QueryType(Enum):
    """ Types of queries supported by the indexer """
    MATCH = 0
    PHRASE = 1


class Query:
    """ Params to use when searching index """
    __slots__ = 'query_type', 'expression', 'offset'

    def __init__(self, query_type: QueryType, expression: str, offset: int):
        self.query_type = query_type
        self.expression = expression
        self.offset = offset


class Document:
    """ Encapsulates all info about a document indexed by FabiSearch """
    __slots__ = 'link_id', 'url', 'title', 'content', 'indexed_at', 'pagerank'

    def __init__(self,
                 link_id: common.UUID,
                 url: str,
                 title: str,
                 content: str,
                 indexed_at: datetime,
                 pagerank: float):
        self.link_id = link_id
        self.url = url
        self.title = title
        self.content = content
        self.indexed_at = indexed_at
        self.pagerank = pagerank

    # def __deepcopy__(self, memo):
    #     id_self = id(self)
    #     copy_self = memo.get(id_self, False)
    #     if copy_self:
    #         return copy_self
    #     copy_self = type(self)(
    #         deepcopy(self.link_id, memo),
    #         deepcopy(self.url, memo),
    #         deepcopy(self.title, memo),
    #         deepcopy(self.document, memo),
    #         deepcopy(self.pagerank, memo),
    #         deepcopy(self.indexed_at, memo))
    #     memo[id_self] = copy_self
    #     return copy_self


class IndexerInterface(metaclass=ABCMeta):
    """ Graph implemented by objects that can mutate or query a link graph """
    @ abstractmethod
    def upsert_doc_index(self, doc: Document) -> Document:
        """ Indexes a new document or updates existing """

    @abstractmethod
    def find_doc_by_link_id(self, link_id: common.UUID) -> Document:
        """ Find document by related link object's link_id """

    @abstractmethod
    def search(self, query: Query) -> Iterator[Document]:
        """ Search index by query and return iterator of docs """

    def update_pagerank_score(self,
                              link_id: common.UUID,
                              pagerank_score: float):
        """ Update doc score for link_id, if not exists, placeholder score """


class IndexerInMemory(IndexerInterface):
    """ Implements Indexer behavior in memory """
