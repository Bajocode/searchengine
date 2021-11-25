"""Indexer."""

from abc import ABCMeta, abstractmethod
from enum import Enum
import common
from datetime import datetime
from copy import deepcopy
from collections import defaultdict
from typing import Iterator
import uuid


class QueryType(Enum):
    """ Types of queries supported by the indexer """
    MATCH = 0
    PHRASE = 1


class Query:
    """ Params to use when searching index """
    __slots__ = 'query_type', 'expression', 'offset'

    def __init__(self,
                 query_type: QueryType = 0,
                 expression: str = '',
                 offset: int = 0):
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
    def upsert_document_index(self, document: Document) -> Document:
        """ Indexes a new document or updates existing """

    @abstractmethod
    def find_document_by_link_id(self, link_id: common.UUID) -> Document:
        """ Find document by related link object's link_id """

    @abstractmethod
    def search_documents(self, query: Query) -> Iterator[Document]:
        """ Search index by query and return iterator of docs """

    @abstractmethod
    def update_pagerank_score(self,
                              link_id: common.UUID,
                              pagerank_score: float):
        """ Update doc score for link_id, if not exists, placeholder score """


class IndexerInMemory(IndexerInterface):
    """ Implements Indexer behavior in memory """

    def __init__(self):
        self.documents: dict[str, Document] = {}
        self.index: dict[str, set[str]] = defaultdict(set)

    def upsert_document_index(self, document: Document) -> Document:
        """ Indexes a new document or updates existing """
        if not document.link_id:
            raise ValueError(f'link_id param missing for doc \n{document}')
        document_copy = deepcopy(document)
        document_copy.indexed_at = datetime.utcnow()
        key = document_copy.link_id
        if key in self.documents:
            document_copy.pagerank = self.documents[key].pagerank
            self._delete_from_index(document)
        self._update_index(document_copy)
        self.documents[key] = document_copy
        return document_copy

    def find_document_by_link_id(self, link_id: common.UUID) -> Document:
        """ Find document by related link object's link_id """
        if link_id in self.documents:
            return deepcopy(self.documents[link_id])
        raise KeyError(f'link_id {link_id} missing indexer.docs')

    def search_documents(self, query: Query) -> Iterator[Document]:
        """ Search index by query and return iterator of docs

            TODO
            - Incorporate Swoosh or Lucene or Lupyne
            - Incorporate Query objects and pagination
            - Allow both AND and OR searches
            - Sort by rank
        """
        result = [self.index.get(token, set())
                  for token in self._tokenize(query.expression)]
        documents = [self.documents[link_id] for link_id in set.union(*result)]
        return iter(documents)

    def update_pagerank_score(self,
                              link_id: common.UUID,
                              pagerank_score: float):
        document = self.documents.setdefault(
            link_id, Document(link_id=link_id))
        document.pagerank = pagerank_score
        self._reindex(document)

    def _update_index(self, document: Document):
        for token in self._tokenize_document(document):
            self.index[token].add(document.link_id)

    def _delete_from_index(self, document: Document):
        for _, link_id_set in self.index.items():
            if document.link_id in link_id_set:
                link_id_set.remove(document.link_id)

    def _reindex(self, document: Document):
        self._delete_from_index(document)
        self._update_index(document)

    def _tokenize_document(self, document: Document) -> list[str]:
        return self._tokenize(document.title) + self._tokenize(document.content)

    def _tokenize(self, text: str) -> list[str]:
        return [token.lower() for token in text.split()]
