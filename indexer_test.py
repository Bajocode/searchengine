import unittest
import graph
import indexer
import uuid
from datetime import datetime, timezone


class IndexerTestCase(unittest.TestCase):
    def setUp(self):
        self.indexer = indexer.IndexerInMemory()

    def test_upsert_doc_index(self):
        doc_original = indexer.Document(
            link_id=uuid.uuid4(),
            url='http://example.com',
            title='Illustrious examples',
            content='Lorem ipsum dolor',
            indexed_at=datetime.utcnow()
        )

        doc_inserted = self.indexer.upsert_doc_index(doc_original)
        self.assertIsNotNone(
            doc_inserted,
            'expected doc to be indexed and returned by copy')


if __name__ == '__main__':
    unittest.main()
