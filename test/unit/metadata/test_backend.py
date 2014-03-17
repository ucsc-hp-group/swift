# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
import unittest
from time import sleep, time
from uuid import uuid4

from swift.account.backend import AccountBroker
from swift.common.utils import normalize_timestamp
from swift.metadata.backend import MetadataBroker


class TestMetadataBroker(unittest.TestCase):

    def test_creation(self):
        # Test MetadataBroker.__init__
        broker = MetadataBroker(':memory:')
        self.assertEqual(broker.db_file, ':memory:')
        got_exc = False
        try:
            with broker.get() as conn:
                pass
        except Exception:
            got_exc = True
        self.assert_(got_exc)
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            curs = conn.cursor()
            curs.execute('SELECT 1')
            self.assertEqual(curs.fetchall()[0][0], 1)
    
    def test_exception(self):
        # Test MetadataBroker throwing a conn away after exception
        first_conn = None
        broker = MetadataBroker(':memory:')
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            first_conn = conn
        try:
            with broker.get() as conn:
                self.assertEqual(first_conn, conn)
                raise Exception('OMG')
        except Exception:
            pass
        self.assert_(broker.conn is None)

    def test_empty(self):
        # Test AccountBroker.empty
        broker = MetadataBroker(':memory:')
        broker.initialize(normalize_timestamp('1'))
        self.assert_(broker.empty())

        #todo: put metadata in
        broker.put_container('o', normalize_timestamp(time()), 0, 0, 0)
        self.assert_(not broker.empty())
        sleep(.00001)

        #todo: remove
        broker.put_container('o', 0, normalize_timestamp(time()), 0, 0)
        self.assert_(broker.empty())
'''
    def get_attributes_query(self, acc, con, obj, attrs):

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

        #queries = ''
        # improper AND
        queries = 'object_name=cat anD object_meta=100'
        thetest = backend.get_uri_query(sql,queries)
        self.assertEquals(thetest,'''
            SELECT something 
            FROM somewhere 
            WHERE object_name=cat AND object_meta=100
        ''')

        #queries = ''
        # improper AND
        queries = 'object_name=cat anD object_meta=100'
        thetest = backend.get_uri_query(sql,queries)
        self.assertEquals(thetest,'''
            SELECT something 
            FROM somewhere 
            WHERE object_name=cat AND object_meta=100
        ''')

        #queries = ''
        # improper AND
        queries = 'object_name=cat anD object_meta=100'
        thetest = backend.get_uri_query(sql,queries)
        self.assertEquals(thetest,'''
            SELECT something 
            FROM somewhere 
            WHERE object_name=cat AND object_meta=100
        ''')

        #queries = ''