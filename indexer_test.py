"""IndexerTestCase"""

import unittest
import indexer
import uuid
from datetime import datetime


class IndexerTestCase(unittest.TestCase):
    """Indexing testcase"""

    def setUp(self):
        self.indexer: indexer.IndexerInMemory = indexer.IndexerInMemory()

    def test_upsert_doc_index(self):
        doc_original = indexer.Document(
            link_id=uuid.uuid4(),
            url='http://example.com',
            title='Illustrious examples',
            content='Lorem ipsum dolor',
            indexed_at=datetime.utcnow(),
            pagerank=5.0
        )
        doc_original_2 = indexer.Document(
            link_id=uuid.uuid4(),
            url='http://example.com',
            title='examples for patatje oorlog',
            content='dikke paark op je ipsum ouwe dolor',
            indexed_at=datetime.utcnow()
        )

        doc_inserted = self.indexer.upsert_document_index(doc_original)
        doc_inserted_2 = self.indexer.upsert_document_index(doc_original_2)
        self.assertIsNotNone(
            doc_inserted,
            'expected doc to be indexed and returned by copy')
        self.assertIsNotNone(
            doc_inserted_2,
            'expected doc to be indexed and returned by copy')
        self.assertNotEqual(id(doc_inserted), id(
            doc_original), 'expected copy to be made')

        # Update doc and assert index is up to date
        doc_update = indexer.Document(
            link_id=doc_original.link_id,
            url='http://example.com',
            title='ajeto',
            content='pijboom',
            indexed_at=datetime.utcnow()
        )
        doc_updated = self.indexer.upsert_document_index(doc_update)
        self.assertEqual(doc_inserted.link_id, doc_updated.link_id)
        self.assertEqual(doc_inserted.pagerank, doc_updated.pagerank)

    def test_find_document_by_link_id(self):
        doc_original = indexer.Document(
            link_id=uuid.uuid4(),
            url='http://example.com',
            title='Illustrious examples',
            content='Lorem ipsum dolor',
            indexed_at=datetime.utcnow(),
            pagerank=5.0
        )
        self.indexer.upsert_document_index(doc_original)
        document_found = self.indexer.find_document_by_link_id(
            doc_original.link_id)
        self.assertIsNotNone(document_found)
        self.assertRaises(
            KeyError, self.indexer.find_document_by_link_id, 'bullshit')

    def test_update_pagerank_score(self):
        self.indexer.update_pagerank_score('bullshit', 5.0)
        document_found = self.indexer.find_document_by_link_id('bullshit')
        self.assertIsNotNone(document_found)
        self.assertEqual(document_found.pagerank, 5.0)
        self.indexer.update_pagerank_score('bullshit', 4.0)
        document_found = self.indexer.find_document_by_link_id('bullshit')
        self.assertEqual(document_found.pagerank, 4.0)

    def test_search_documents(self):
        doc_original = indexer.Document(
            link_id='bullshit',
            url='http://example.com',
            title='Illustrious examples',
            content='Lorem ipsum dolor',
            indexed_at=datetime.utcnow(),
            pagerank=5.0
        )
        self.indexer.upsert_document_index(doc_original)
        document_iter = self.indexer.search_documents(
            indexer.Query(expression='examples')
        )
        self.assertEqual(list(document_iter)[0].link_id, 'bullshit')
        document_iter = self.indexer.search_documents(
            indexer.Query(expression='example')
        )
        self.assertTrue(not list(document_iter))


if __name__ == '__main__':
    unittest.main()
