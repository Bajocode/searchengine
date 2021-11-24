"""A one line summary of the module or program, terminated by a period.

Leave one blank line.  The rest of this docstring should contain an
overall description of the module or program.  Optionally, it may also
contain a brief description of exported classes and functions and/or usage
examples.

  Typical usage example:

  foo = ClassFoo()
  bar = foo.FunctionBar()
"""
import time
import uuid
from typing import Iterator
from copy import deepcopy
from collections import defaultdict
import graph
import common

EdgeList = list[common.UUID]


class GraphInMemory(graph.GraphInterface):
    """ Implements graph interface (TODO: synchronize)

    Attributes:
        links: dict[UUID, Link] for inserted link models, link_id as key
        edges: dict[UUID, Edge] for inserted edge models, edge_id as key
        link_url_index: dict[str, Link] ensure link.url unique, refer self.links
        link_edge_map: dict[UUID, EdgeList] efficient get all edges for link_id
    """

    links: dict[common.UUID, graph.Link] = {}
    edges: dict[common.UUID, graph.Edge] = {}
    link_url_index: dict[str, graph.Link] = {}
    link_edge_map: dict[common.UUID, EdgeList] = defaultdict(list)

    def find_link(self, link_id: common.UUID) -> graph.Link:
        """ Returns deepcopy of link else raises NotFound """
        link = self.links.get(link_id, False)
        if not link:
            raise common.NotFound('find link:')
        return deepcopy(link)

    def upsert_link(self, link: graph.Link) -> graph.Link:
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
        link_copy.link_id = str(uuid.uuid4())
        while self.links.get(link_copy.link_id, False):
            link_copy.link_id = str(uuid.uuid4())
        self.link_url_index[link_copy.url] = link_copy
        self.links[link_copy.link_id] = link_copy
        return link_copy

    def links_iter(self,
                   from_id: common.UUID,
                   to_id: common.UUID,
                   retrieved_before: common.Timestamp) -> Iterator[graph.Link]:
        return (link for link in self.links.values()
                if from_id <= link.link_id < to_id
                and link.retrieved_at < retrieved_before)

    def upsert_edge(self, edge: graph.Edge) -> graph.Edge:
        """ Updates or inserts new edge into mem store
            - Traverse edges that originate from src and check ifexists edge
            with same destination
            - If so, update updated_at and copy vals back to param edge
            - This syncs provided edge's id and updated_at with mem store
        """
        if edge.src not in self.links or \
                edge.dst not in self.links:
            raise common.UnknownEdgeLinks('Upsert edge:')

        for edge_id in self.link_edge_map.get(edge.src, []):
            edge_stored = self.edges[edge_id]
            if edge_stored.src == edge.src and \
                    edge_stored.dst == edge.dst:
                edge_stored.updated_at = int(time.time())
                return deepcopy(edge_stored)

        # Insert new edge and ensure unique uuid
        edge_copy = deepcopy(edge)
        edge_copy.edge_id = str(uuid.uuid4())
        while self.edges.get(edge_copy.edge_id, False):
            edge_copy.edge_id = str(uuid.uuid4())
        edge_copy.updated_at = int(time.time())
        self.edges[edge_copy.edge_id] = edge_copy
        self.link_edge_map[edge_copy.src] += [edge_copy.edge_id]
        return edge_copy

    def edges_iter(self,
                   from_id: common.UUID,
                   to_id: common.UUID,
                   updated_before: common.Timestamp) -> Iterator[graph.Edge]:
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
                           from_id: common.UUID,
                           deletion_treshold: common.Timestamp):
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
