"""Graph."""

import uuid
from abc import ABCMeta, abstractmethod
from typing import Iterator
from datetime import datetime
from copy import deepcopy
from collections import defaultdict

EdgeList = list[uuid.UUID]


class Link:
    """ Encapsulates all information about a link discovered by crawler """
    __slots__ = 'link_id', 'url', 'retrieved_at'

    def __init__(self,
                 link_id: uuid.UUID = uuid.UUID(int=0),
                 url: str = '',
                 retrieved_at: datetime = datetime.min):
        self.link_id = link_id
        self.url = url
        self.retrieved_at = retrieved_at

    def __eq__(self, other):
        return self.link_id == other.link_id \
            and self.url == other.url \
            and self.retrieved_at == other.retrieved_at

    def __repr__(self) -> str:
        return f'\n\
            link_id:\t{self.link_id}\n\
            url:\t{self.url}\n\
            retrieved_at:\t{self.retrieved_at}\n'


class Edge:
    """ A graph edge that originates from src and terminated at dst """
    __slots__ = 'edge_id', 'src', 'dst', 'updated_at'

    def __init__(self,
                 edge_id: uuid.UUID = uuid.UUID(int=0),
                 src: uuid.UUID = uuid.UUID(int=0),
                 dst: uuid.UUID = uuid.UUID(int=0),
                 updated_at: datetime = datetime.min):
        self.edge_id = edge_id
        self.src = src
        self.dst = dst
        self.updated_at = updated_at

    def __eq__(self, other):
        return self.edge_id == other.edge_id \
            and self.src == other.src \
            and self.dst == other.dst \
            and self.updated_at == other.updated_at

    def __repr__(self) -> str:
        return f'\n\
            edge_id:\t{self.edge_id}\n\
            src:\t{self.src}\n\
            dst:\t{self.dst}\n\
            updated_at:\t{self.updated_at}\n'


class Graph(metaclass=ABCMeta):
    """ Graph implemented by objects that can mutate or query a link graph """
    @ abstractmethod
    def upsert_link(self, link: Link) -> Link:
        """ Creates a new link or updates existing """

    @ abstractmethod
    def find_link(self, link_id: uuid.UUID) -> Link:
        """ Looks up a link by its id """

    @ abstractmethod
    def links_iter(self,
                   from_id: uuid.UUID,
                   to_id: uuid.UUID,
                   retrieved_before: datetime) -> Iterator[Link]:
        """ Returns iterator for links between from, to before time """

    @ abstractmethod
    def upsert_edge(self, edge: Edge) -> Edge:
        """ Creates new or updates existing edge """

    @ abstractmethod
    def edges_iter(self,
                   from_id: uuid.UUID,
                   to_id: uuid.UUID,
                   updated_before: datetime) -> Iterator[Edge]:
        """ Returns edges for vertice ids between from, to before time """

    @ abstractmethod
    def remove_stale_edges(self,
                           from_id: uuid.UUID,
                           deletion_treshold: datetime):
        """ Removes edges for links between from, to before time

        iterate the list of edges that originate from the specified source link
        and ignore the ones whose UpdatedAt value is less than the specified
        updatedBefore argument. Any edge that survives the culling is added to
        a newEdgeList, which becomes the new list of outgoing edges for the
        specified source link
        """


class GraphInMemory(Graph):
    """ Implements graph interface (TODO: synchronize)

    Attributes:
        links: dict[UUID, Link] for inserted link models, link_id as key
        edges: dict[UUID, Edge] for inserted edge models, edge_id as key
        link_url_index: dict[str, Link] ensure link.url unique, refer self.links
        link_edge_map: dict[UUID, EdgeList] efficient get all edges for link_id
    """

    def __init__(self,
                 links: dict[uuid.UUID, Link] = {},
                 edges: dict[uuid.UUID, Edge] = {},
                 link_url_index: dict[str, Link] = {},
                 link_edge_map: dict[uuid.UUID, EdgeList] = defaultdict(list)):
        self.links = links
        self.edges = edges
        self.link_url_index = link_url_index
        self.link_edge_map = link_edge_map

    def find_link(self, link_id: uuid.UUID) -> Link:
        """ Returns deepcopy of link else raises NotFound """
        link = self.links.get(link_id, False)
        if not link:
            raise KeyError('find_link(link_id={link_id})')
        return deepcopy(link)

    def upsert_link(self, link: Link) -> Link:
        """
        If link with same URL already exists, execute update
        else assign new id and insert link
        """
        if link.url in self.link_url_index:
            link_stored = self.link_url_index[link.url]
            link_stored.retrieved_at = max(
                link_stored.retrieved_at,
                link.retrieved_at)
            return deepcopy(link_stored)

        link_copy = deepcopy(link)
        link_copy.link_id = uuid.uuid4()
        while self.links.get(link_copy.link_id, False):
            link_copy.link_id = uuid.uuid4()
        self.link_url_index[link_copy.url] = link_copy
        self.links[link_copy.link_id] = link_copy
        return link_copy

    def links_iter(self,
                   from_id: uuid.UUID,
                   to_id: uuid.UUID,
                   retrieved_before: datetime) -> Iterator[Link]:
        return (link for link in deepcopy(self.links).values()
                if from_id <= link.link_id < to_id
                and link.retrieved_at < retrieved_before)

    def upsert_edge(self, edge: Edge) -> Edge:
        """ Updates or inserts new edge into mem store
            - Traverse edges that originate from src and check ifexists edge
            with same destination
            - If so, update updated_at and copy vals back to param edge
            - This syncs provided edge's id and updated_at with mem store
        """
        if edge.src not in self.links or \
                edge.dst not in self.links:
            raise KeyError('Edge src or dst not in stored links')

        for edge_id in self.link_edge_map.get(edge.src, []):
            edge_stored = self.edges[edge_id]
            if edge_stored.src == edge.src and \
                    edge_stored.dst == edge.dst:
                edge_stored.updated_at = datetime.utcnow()
                return deepcopy(edge_stored)

        # Insert new edge and ensure unique uuid
        edge_copy = deepcopy(edge)
        edge_copy.edge_id = uuid.uuid4()
        while self.edges.get(edge_copy.edge_id, False):
            edge_copy.edge_id = uuid.uuid4()
        edge_copy.updated_at = datetime.utcnow()
        self.edges[edge_copy.edge_id] = edge_copy
        self.link_edge_map[edge_copy.src] += [edge_copy.edge_id]
        return edge_copy

    def edges_iter(self,
                   from_id: uuid.UUID,
                   to_id: uuid.UUID,
                   updated_before: datetime) -> Iterator[Edge]:
        result = []
        for link_id in self.links:
            if link_id < from_id or link_id >= to_id:
                continue
            for edge_id in self.link_edge_map[link_id]:
                edge = self.edges.get(edge_id, False)
                if edge and edge.updated_at < updated_before:
                    result.append(edge)
        return iter(result)

    def remove_stale_edges(self,
                           from_id: uuid.UUID,
                           deletion_treshold: datetime):
        new_edgelist: EdgeList = []
        for edge_id in self.link_edge_map[from_id]:
            edge_stored = self.edges[edge_id]
            # if stored edge is earlier updated than given time
            if edge_stored.updated_at < deletion_treshold:
                del self.edges[edge_id]
                continue
            # otherwise append to new list
            new_edgelist.append(edge_id)
        self.link_edge_map[from_id] = new_edgelist
