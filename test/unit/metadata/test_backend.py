import hashlib
import unittest
from time import sleep, time
from uuid import uuid4

from swift.account.backend import AccountBroker
from swift.common.utils import normalize_timestamp
from swift.metadata.backend import MetadataBroker

class TestMetadataBroker(unittest.TestCase):
    #ef setUp(self):
     #   broker = MetadataBroker

    #def tearDown(self):

    '''
    def test_exception(self):

    def test_empty(self):

    def test_query_with_attrs(self):

    def test_query_with_attrs_and_md_queries(self):

    '''
    def test_get_uri_query(self, sql, queries):
        sql = """
            SELECT something 
            FROM somewhere
        """
        # proper input
        queries = 'object_name=cat AND object_meta=100'
        thetest = backend.get_uri_query(sql,queries)
        self.assertEquals(thetest,'''
            SELECT something 
            FROM somewhere 
            WHERE object_name=cat AND object_meta=100
        ''')

        # improper AND
        queries = 'object_name=cat anD object_meta=100'
        thetest = backend.get_uri_query(sql,queries)
        self.assertEquals(thetest,'''
            SELECT something 
            FROM somewhere 
            WHERE object_name=cat AND object_meta=100
        ''')

        #queries = ''