'''A one line summary of the module or program, terminated by a period.

Leave one blank line.  The rest of this docstring should contain an
overall description of the module or program.  Optionally, it may also
contain a brief description of exported classes and functions and/or usage
examples.

  Typical usage example:

  foo = ClassFoo()
  bar = foo.FunctionBar()
'''
import psycopg2
from psycopg2.errors import SerializationFailure
import graph
import common
from typing import Iterator
import datetime

CDB_DSN = 'postgresql://root:81f2a0bc1c88@localhost:26257/defaultdb?sslmode=disable'


class GraphCockroachDb(graph.GraphInterface):
    def __init__(self):
        self.conn = psycopg2.connect(CDB_DSN)

    def upsert_link(self, link: graph.Link) -> graph.Link:
        query = f'''INSERT INTO links (url, retrieved_at) VALUES ({link.url}, {link.retrieved_at})
        ON CONFLICT (url) DO UPDATE SET retrieved_at=GREATEST(links.retrieved_at, {link.retrieved_at})
        RETURNING id, retrieved_at;
        '''
        query = 'INSERT INTO links (url, retrieved_at) VALUES (%s, %s);'
        result = self._execute_query(
            query, ('bullshit', datetime.datetime.now(datetime.timezone.utc)))
        print(result)

    def find_link(self, link_id: common.UUID) -> graph.Link:
        pass

    def links_iter(self,
                   from_id: common.UUID,
                   to_id: common.UUID,
                   retrieved_before: common.Timestamp) -> Iterator[graph.Link]:
        pass

    def upsert_edge(self, edge: graph.Edge) -> graph.Edge:
        pass

    def edges_iter(self,
                   from_id: common.UUID,
                   to_id: common.UUID,
                   updated_before: common.Timestamp) -> Iterator[graph.Edge]:
        pass

    def remove_stale_edges(self,
                           from_id: common.UUID,
                           deletion_treshold: common.Timestamp):
        pass

    def _execute_query(self, query: str, params: tuple) -> list:
        rows = []
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            self.conn.commit()
        return rows


g = GraphCockroachDb()
g.upsert_link(graph.Link(url='bullshit', retrieved_at=0))
