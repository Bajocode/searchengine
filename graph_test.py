import unittest
import graph
import time
import uuid


class GraphTestCase(unittest.TestCase):
    def setUp(self):
        self.g = graph.GraphInMemory()

    def test_upsert_link(self):
        link_original = graph.Link(
            url='https://example.com',
            retrieved_at=int(time.time())
        )

        link_inserted = self.g.upsert_link(link_original)
        self.assertIsNotNone(
            link_inserted.link_id,
            'expected a linkID to be assigned to the new link')
        self.assertNotEqual(
            link_original.link_id,
            link_inserted.link_id,
            'expected not to change link outside func scope')

        # Update existing link with a newer timestamp and different URL
        accessed_at = int(time.time())
        link_stored = graph.Link(
            url='https://example.com',
            retrieved_at=accessed_at)
        link_stored.link_id = link_inserted.link_id
        link_updated = self.g.upsert_link(link_stored)
        self.assertEqual(
            link_inserted.link_id,
            link_updated.link_id,
            'link ID changed while upserting')

        link_found = self.g.find_link(link_updated.link_id)
        self.assertEqual(link_found.retrieved_at, accessed_at)

        # Attempt insert new link with existing url, while has later accessed_at
        link_sameurl = graph.Link(
            url=link_stored.url,
            retrieved_at=int(time.time()))
        link_sameurl_updated = self.g.upsert_link(link_sameurl)
        self.assertEqual(link_sameurl_updated.link_id, link_updated.link_id)

        link_found = self.g.find_link(link_stored.link_id)
        self.assertEqual(link_found.retrieved_at,
                         accessed_at,
                         'last accessed stamp was overwritten with older value')

        #  Create new link and attempt update url to same an existing link
        link_dup = graph.Link(url='foo')
        link_dup_inserted = self.g.upsert_link(link_dup)
        self.assertNotEqual(
            link_dup_inserted.link_id,
            str(uuid.UUID(int=0)),
            'expected a linkID to be assigned to the new link')

    def test_find_link(self):
        link_original = graph.Link(
            url='https://example.com',
            retrieved_at=int(time.time())
        )

        link_inserted = self.g.upsert_link(link_original)
        self.assertNotEqual(link_inserted.link_id, str(uuid.UUID(int=0)))

        # Lookup link by valid id
        link_found = self.g.find_link(link_inserted.link_id)

        self.assertEqual(link_inserted,
                         link_found,
                         'lookup by ID returned the wrong link')

    def test_upsert_edge(self):
        link_ids = []
        for i in range(3):
            link = graph.Link(url=str(i))
            link_ids.append(self.g.upsert_link(link).link_id)

        # Insert a new edge and assert id and updated fields
        edge_original = graph.Edge(
            src=link_ids[0],
            dst=link_ids[1]
        )
        edge_inserted = self.g.upsert_edge(edge_original)
        self.assertNotEqual(edge_inserted.edge_id, uuid.UUID(
            int=0), 'Expected edge_id to be assigned')
        self.assertNotEqual(edge_inserted.updated_at, 0, 'updated_at not set')

        # Update edge and assert id same but updated field changed
        edge_samelinks = graph.Edge(
            edge_id=edge_inserted.edge_id,
            src=link_ids[0],
            dst=link_ids[1]
        )
        edge_updated = self.g.upsert_edge(edge_samelinks)
        self.assertEqual(edge_updated.edge_id, edge_samelinks.edge_id,
                         'edge_id modified while updating')
        self.assertNotEqual(edge_updated.updated_at,
                            edge_samelinks.updated_at,
                            'update_at not modified')

    def test_links_iter(self):
        for i in range(50):
            link = graph.Link(url=str(i))
            self.assertIsNotNone(self.g.upsert_link(link))
        from_id, to_id, second_later = '0', '1', int(time.time()) + 1
        linksiter = self.g.links_iter(
            from_id=from_id, to_id=to_id, retrieved_before=second_later)
        filtered = [link for link in self.g.links.values()
                    if from_id <= link.link_id < to_id
                    and link.retrieved_at < second_later]

        self.assertEqual(
            sorted(list(linksiter), key=lambda link: link.url),
            sorted(filtered, key=lambda link: link.url))

    def test_edges_iter(self):
        for i in range(50):
            src = self.g.upsert_link(link=graph.Link(url=str(-i)))
            dst = self.g.upsert_link(link=graph.Link(url=str(i)))
            edge = graph.Edge(src=src.link_id, dst=dst.link_id)
            self.g.upsert_edge(edge=edge)

        from_id, to_id, second_later = '0', '1', int(time.time()) + 1
        edgesiter = self.g.edges_iter(
            from_id=from_id, to_id=to_id, updated_before=second_later)
        filtered = []
        for link_id in self.g.links:
            if link_id < from_id or link_id >= to_id:
                continue
            for edge_id in self.g.link_edge_map[link_id]:
                edge = self.g.edges.get(edge_id, False)
                if edge and edge.updated_at < second_later:
                    filtered.append(edge)

        self.assertEqual(
            sorted(list(edgesiter), key=lambda edge: edge.edge_id),
            sorted(filtered, key=lambda edge: edge.edge_id))

    def test_remove_stale_edges(self):
        src_ids = []
        for i in range(3):
            src = self.g.upsert_link(link=graph.Link(url=str(-i)))
            src_ids.append(src.link_id)
            dst = self.g.upsert_link(link=graph.Link(url=str(i)))
            edge = graph.Edge(src=src.link_id, dst=dst.link_id)
            self.g.upsert_edge(edge=edge)

        from_id = src_ids[1]
        edge_ids_before = list(self.g.link_edge_map[from_id])
        second_later = int(time.time()) + 1
        self.g.remove_stale_edges(
            from_id=from_id, deletion_treshold=second_later)
        self.assertNotEqual(edge_ids_before, self.g.link_edge_map[from_id])


if __name__ == '__main__':
    unittest.main()
