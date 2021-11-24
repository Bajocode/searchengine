"""A one line summary of the module or program, terminated by a period.

Leave one blank line.  The rest of this docstring should contain an
overall description of the module or program.  Optionally, it may also
contain a brief description of exported classes and functions and/or usage
examples.

  Typical usage example:

  foo = ClassFoo()
  bar = foo.FunctionBar()
"""

import uuid
import time
from abc import ABCMeta, abstractmethod
from typing import Iterator
from copy import deepcopy
import common


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

    # def __deepcopy__(self, memo):  # memo is a dict of id's to copies
    #     id_self = id(self)        # memoization avoids unnecesary recursion
    #     copy_self = memo.get(id_self, False)
    #     if copy_self:
    #         return copy_self
    #     copy_self = type(self)(
    #         deepcopy(self.link_id, memo),
    #         deepcopy(self.url, memo),
    #         deepcopy(self.retrieved_at, memo))
    #     memo[id_self] = copy_self
    #     return copy_self

    def __repr__(self) -> str:
        return f'\n\
            link_id:\t{self.link_id}\n\
            url:\t{self.url}\n\
            retrieved_at:\t{self.retrieved_at}\n'


class Edge:
    """ A graph edge that originates from src and terminated at dst """
    __slots__ = 'edge_id', 'src', 'dst', 'updated_at'

    def __init__(self,
                 edge_id: common.UUID = str(uuid.UUID(int=0)),
                 src: common.UUID = str(uuid.UUID(int=0)),
                 dst: common.UUID = str(uuid.UUID(int=0)),
                 updated_at: common.Timestamp = 0):
        self.edge_id = edge_id
        self.src = src
        self.dst = dst
        self.updated_at = updated_at

    def __eq__(self, other):
        return self.edge_id == other.edge_id \
            and self.src == other.src \
            and self.dst == other.dst \
            and self.updated_at == other.updated_at

    # def __deepcopy__(self, memo):  # memo is a dict of id's to copies
    #     id_self = id(self)        # memoization avoids unnecesary recursion
    #     copy_self = memo.get(id_self, False)
    #     if copy_self:
    #         return copy_self
    #     copy_self = type(self)(
    #         deepcopy(self.edge_id, memo),
    #         deepcopy(self.src, memo),
    #         deepcopy(self.dst, memo),
    #         deepcopy(self.updated_at, memo))
    #     memo[id_self] = copy_self
    #     return copy_self

    def __repr__(self) -> str:
        return f'\n\
            edge_id:\t{self.edge_id}\n\
            src:\t{self.src}\n\
            dst:\t{self.dst}\n\
            updated_at:\t{self.updated_at}\n'


class GraphInterface(metaclass=ABCMeta):
    """ Graph implemented by objects that can mutate or query a link graph """
    @ abstractmethod
    def upsert_link(self, link: Link) -> Link:
        """ Creates a new link or updates existing """

    @ abstractmethod
    def find_link(self, link_id: common.UUID) -> Link:
        """ Looks up a link by its id """

    @ abstractmethod
    def links_iter(self,
                   from_id: common.UUID,
                   to_id: common.UUID,
                   retrieved_before: common.Timestamp) -> Iterator[Link]:
        """ Returns iterator for links between from, to before time """

    @ abstractmethod
    def upsert_edge(self, edge: Edge) -> Edge:
        """ Creates new or updates existing edge """

    @ abstractmethod
    def edges_iter(self,
                   from_id: common.UUID,
                   to_id: common.UUID,
                   updated_before: common.Timestamp) -> Iterator[Edge]:
        """ Returns edges for vertice ids between from, to before time """

    @ abstractmethod
    def remove_stale_edges(self,
                           from_id: common.UUID,
                           deletion_treshold: common.Timestamp):
        """ Removes edges for links between from, to before time

        iterate the list of edges that originate from the specified source link
        and ignore the ones whose UpdatedAt value is less than the specified
        updatedBefore argument. Any edge that survives the culling is added to
        a newEdgeList, which becomes the new list of outgoing edges for the
        specified source link
        """
