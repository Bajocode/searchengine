"""Indexer."""

from abc import ABCMeta, abstractmethod
from enum import Enum
import common
from datetime import datetime
from copy import deepcopy
from typing import Iterator
import uuid


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
                 link_id: common.UUID = str(uuid.UUID(int=0)),
                 url: str = '',
                 title: str = '',
                 content: str = '',
                 indexed_at: datetime = datetime.min,
                 pagerank: float = 0.0):
        self.link_id = link_id
        self.url = url
        self.title = title
        self.content = content
        self.indexed_at = indexed_at
        self.pagerank = pagerank

    def __repr__(self) -> str:
        return f'''
            link_id:\t{self.link_id}
            url:\t{self.url}
            title:\t{self.title}
            content:\t{self.content}
            indexed_at:\t{self.indexed_at}
            pagerank: \t{self.pagerank}'''


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

    @abstractmethod
    def update_pagerank_score(self,
                              link_id: common.UUID,
                              pagerank_score: float):
        """ Update doc score for link_id, if not exists, placeholder score """


class IndexerInMemory(IndexerInterface):
    """ Implements Indexer behavior in memory """

    def upsert_doc_index(self, doc: Document) -> Document:
        """ Indexes a new document or updates existing """
        pass

    def find_doc_by_link_id(self, link_id: common.UUID) -> Document:
        """ Find document by related link object's link_id """
        pass

    def search(self, query: Query) -> Iterator[Document]:
        """ Search index by query and return iterator of docs """
        pass

    def update_pagerank_score(self,
                              link_id: common.UUID,
                              pagerank_score: float):
        pass
