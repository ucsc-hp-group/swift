import errno
import os
import mock
import unittest
import time
from shutil import rmtree
from StringIO import StringIO

import simplejson
import xml.dom.minidom

from swift.common.swob import Request
# from swift.account.server import AccountController, ACCOUNT_LISTING_LIMIT
from swift.metadata.server import MetadataController

from swift.common.utils import normalize_timestamp, replication, public, json
from swift.common.request_helpers import get_sys_meta_prefix


class TestMetadataController(unittest.TestCase):

    def setUp(self):
        """
        Temp dir is created and location stuff is set up for the controller
        """
        self.testDir = os.path.join(os.path.dirname(__file__), 
            'metadata_controller')
        self.controller = MetadataController({
                'location': self.testDir,
                'db_file' : os.path.join(self.testDir, 'meta.db')
            })
        self.t = normalize_timestamp(time.time())
        self.test_uploadDefault()

    def tearDown(self):
        """
        Kill the temp dir
        """
        try:
            rmtree(self.testDir)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise

    def uploadObj(self, a, c, o):
        metaListO = [self.getTestObjDict(a,c,o)]
        reqO = Request.blank(
            '/', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'}, headers={'user-agent': 'object_crawler'},
            body=json.dumps(metaListO))
        respO = reqO.get_response(self.controller)
        self.assert_(respO.status.startswith('204'))

    def uploadCon(self, a, c):
        metaListC = [self.getTestConDict(a,c)]
        reqC = Request.blank(
            '/', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'}, headers={'user-agent': 'container_crawler'},
            body=json.dumps(metaListC))
        respC = reqC.get_response(self.controller)
        self.assert_(respC.status.startswith('204'))

    def uploadAcc(self, a):
        metaListA = [self.getTestAccDict(a)]
        reqA = Request.blank(
            '/', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'}, headers={'user-agent': 'account_crawler'},
            body=json.dumps(metaListA))
        respA = reqA.get_response(self.controller)
        self.assert_(respA.status.startswith('204'))

    def test_uploadDefault(self):
        """
        Uploads:
            TEST_acc1
            TEST_con1 - acc1
            TEST_con2 - acc1
            TEST_obj1 - acc1/con1
            TEST_obj2 - acc1/con1
            TEST_obj3 - acc1/con2
        """
        self.uploadObj(1,1,1)
        self.uploadObj(1,1,2)
        self.uploadObj(1,2,3)
        self.uploadCon(1,1)
        self.uploadCon(1,2)
        self.uploadAcc(1)

    def test_GET_ACCscope_objAttrs_metadata(self):
        """
        In account scope, give me object attrs
        Should return info for all 3 objects
        we uploaded in setup
        """
        attrs = ("object_uri,object_name,object_account_name,"
            "object_container_name,object_uri_create_time,object_etag_hash,object_content_type,"
            "object_content_length,object_content_encoding,object_content_language,"
            "object_meta_TESTCUSTOM")
        # attrs = "all_attrs"
        req2 = Request.blank(
            '/v1/TEST_acc1', environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp2 = req2.get_response(self.controller)
        self.assert_(resp2.status.startswith('200'))
        testList = json.loads(resp2.body)
        self.assert_(len(testList) == 3)
        testDict = testList[0]
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj1']
        self.assertEquals(metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj1')
        self.assertEquals(metaReturned['object_name'], 'TEST_obj1')
        self.assertEquals(metaReturned['object_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['object_container_name'], 'TEST_con1')
        self.assertEquals(metaReturned['object_uri_create_time'], self.t)
        self.assertEquals(metaReturned['object_etag_hash'], '0000000000000000')
        self.assertEquals(metaReturned['object_content_type'], 'text/plain')
        self.assertEquals(str(metaReturned['object_content_length']), '42')
        self.assertEquals(metaReturned['object_content_encoding'], 'gzip')
        self.assertEquals(metaReturned['object_content_language'], 'en')
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'COOL')

        testDict = testList[1]
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj2' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj2']
        self.assertEquals(metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj2')
        self.assertEquals(metaReturned['object_name'], 'TEST_obj2')
        self.assertEquals(metaReturned['object_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['object_container_name'], 'TEST_con1')
        self.assertEquals(metaReturned['object_uri_create_time'], self.t)
        self.assertEquals(metaReturned['object_etag_hash'], '0000000000000000')
        self.assertEquals(metaReturned['object_content_type'], 'text/plain')
        self.assertEquals(str(metaReturned['object_content_length']), '42')
        self.assertEquals(metaReturned['object_content_encoding'], 'gzip')
        self.assertEquals(metaReturned['object_content_language'], 'en')
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'COOL')

        testDict = testList[2]
        self.assert_('/TEST_acc1/TEST_con2/TEST_obj3' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con2/TEST_obj3']
        self.assertEquals(metaReturned['object_uri'], '/TEST_acc1/TEST_con2/TEST_obj3')
        self.assertEquals(metaReturned['object_name'], 'TEST_obj3')
        self.assertEquals(metaReturned['object_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['object_container_name'], 'TEST_con2')
        self.assertEquals(metaReturned['object_uri_create_time'], self.t)
        self.assertEquals(metaReturned['object_etag_hash'], '0000000000000000')
        self.assertEquals(metaReturned['object_content_type'], 'text/plain')
        self.assertEquals(str(metaReturned['object_content_length']), '42')
        self.assertEquals(metaReturned['object_content_encoding'], 'gzip')
        self.assertEquals(metaReturned['object_content_language'], 'en')
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'COOL')

    def test_GET_CONscope_objAttrs_metadata(self):
        """
        In container scope give me object attrs
        Should give back the 2 objects in container 1
        """
        attrs = ("object_uri,object_name,object_account_name,"
            "object_container_name,object_uri_create_time,object_etag_hash,object_content_type,"
            "object_content_length,object_content_encoding,object_content_language,"
            "object_meta_TESTCUSTOM")
        # attrs = "all_attrs"
        req2 = Request.blank(
            '/v1/TEST_acc1/TEST_con1', environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp2 = req2.get_response(self.controller)
        self.assert_(resp2.status.startswith('200'))
        testList = json.loads(resp2.body)
        self.assert_(len(testList) == 2)
        testDict = testList[0]
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj1']
        self.assertEquals(metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj1')
        self.assertEquals(metaReturned['object_name'], 'TEST_obj1')
        self.assertEquals(metaReturned['object_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['object_container_name'], 'TEST_con1')
        self.assertEquals(metaReturned['object_uri_create_time'], self.t)
        self.assertEquals(metaReturned['object_etag_hash'], '0000000000000000')
        self.assertEquals(metaReturned['object_content_type'], 'text/plain')
        self.assertEquals(str(metaReturned['object_content_length']), '42')
        self.assertEquals(metaReturned['object_content_encoding'], 'gzip')
        self.assertEquals(metaReturned['object_content_language'], 'en')
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'COOL')

        testDict = testList[1]
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj2' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj2']
        self.assertEquals(metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj2')
        self.assertEquals(metaReturned['object_name'], 'TEST_obj2')
        self.assertEquals(metaReturned['object_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['object_container_name'], 'TEST_con1')
        self.assertEquals(metaReturned['object_uri_create_time'], self.t)
        self.assertEquals(metaReturned['object_etag_hash'], '0000000000000000')
        self.assertEquals(metaReturned['object_content_type'], 'text/plain')
        self.assertEquals(str(metaReturned['object_content_length']), '42')
        self.assertEquals(metaReturned['object_content_encoding'], 'gzip')
        self.assertEquals(metaReturned['object_content_language'], 'en')
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'COOL')

    def test_GET_OBJscope_objAttrs_metadata(self):
        """
        In object scope give me object attrs
        Should give back the object in the path
        """
        attrs = ("object_uri,object_name,object_account_name,"
            "object_container_name,object_uri_create_time,object_etag_hash,object_content_type,"
            "object_content_length,object_content_encoding,object_content_language,"
            "object_meta_TESTCUSTOM")
        # attrs = "all_attrs"
        req2 = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1', environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp2 = req2.get_response(self.controller)
        self.assert_(resp2.status.startswith('200'))
        testList = json.loads(resp2.body)
        self.assert_(len(testList) == 1)
        testDict = testList[0]
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj1']
        self.assertEquals(metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj1')
        self.assertEquals(metaReturned['object_name'], 'TEST_obj1')
        self.assertEquals(metaReturned['object_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['object_container_name'], 'TEST_con1')
        self.assertEquals(metaReturned['object_uri_create_time'], self.t)
        self.assertEquals(metaReturned['object_etag_hash'], '0000000000000000')
        self.assertEquals(metaReturned['object_content_type'], 'text/plain')
        self.assertEquals(str(metaReturned['object_content_length']), '42')
        self.assertEquals(metaReturned['object_content_encoding'], 'gzip')
        self.assertEquals(metaReturned['object_content_language'], 'en')
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'COOL')

    def getTestObjDict(self, accNum, conNum, objNum):
        metadata = {}
        uri = "/TEST_acc" + str(accNum) + "/TEST_con" + str(conNum) + "/TEST_obj" + str(objNum)
        uri_list = uri.split("/")
        metadata['object_uri'] = uri
        metadata['object_name'] = ("/".join(uri_list[3:]))
        metadata['object_account_name'] = uri_list[1]
        metadata['object_container_name'] = uri_list[2]
        metadata['object_location'] = 'NULL' #Not implemented yet
        metadata['object_uri_create_time'] = self.t
        #Uri create needs to be implemented on meta server.
        metadata['object_last_modified_time'] = self.t
        metadata['object_last_changed_time'] = self.t
        metadata['object_delete_time'] = self.t
        metadata['object_last_activity_time'] = self.t
        metadata['object_etag_hash'] = "0000000000000000"
        metadata['object_content_type'] = "text/plain"
        metadata['object_content_length'] = "42"
        metadata['object_content_encoding'] = "gzip"
        metadata['object_content_disposition'] = "object_content_disposition"
        metadata['object_content_language'] = "en"
        metadata['object_cache_control'] = 'NULL' #Not Implemented yet
        metadata['object_delete_at'] = self.t
        metadata['object_manifest_type'] = 'NULL'  #Not Implemented yet
        metadata['object_manifest'] = 'NULL'  #Not Implemented yet
        metadata['object_access_control_allow_origin'] = 'NULL'  #Not Implemented yet
        metadata['object_access_control_allow_credentials'] = 'NULL' #Not Implemented yet
        metadata['object_access_control_expose_headers'] = 'NULL' #Not Implemented yet
        metadata['object_access_control_max_age'] = 'NULL' #Not Implemented yet
        metadata['object_access_control_allow_methods'] = 'NULL' #Not Implemented yet
        metadata['object_access_control_allow_headers'] = 'NULL' #Not Implemented yet
        metadata['object_origin'] = 'NULL' #Not Implemented yet
        metadata['object_access_control_request_method'] = 'NULL' #Not Implemented yet
        metadata['object_access_control_request_headers'] = 'NULL' #Not Implemented yet
        metadata['object_meta_TESTCUSTOM'] = 'COOL'
        return metadata

    def getTestConDict(self, accNum, conNum):
        metadata = {}
        uri = "/TEST_acc" + str(accNum) + "/TEST_con" + str(conNum)
        metadata['container_uri'] = uri
        metadata['container_name'] = "TEST_con" + str(conNum)
        metadata['container_account_name'] = "TEST_acc" + str(accNum)
        metadata['container_create_time'] = self.t
        metadata['container_last_modified_time'] = self.t
        metadata['container_last_changed_time'] = self.t
        metadata['container_delete_time'] = self.t
        metadata['container_last_activity_time'] = self.t
        metadata['container_read_permissions'] ='NULL' #Not Implemented yet
        metadata['container_write_permissions'] ='NULL' #Not Implemented yet
        metadata['container_sync_to'] = 'NULL'
        metadata['container_sync_key'] = 'NULL'
        metadata['container_versions_location'] = 'NULL' #Not Implemented yet
        metadata['container_object_count'] = '33'
        metadata['container_bytes_used'] = '3342'
        metadata['container_delete_at'] = self.t
        return metadata

    def getTestAccDict(self, accNum):
        metadata = {}
        uri = "/TEST_acc" + str(accNum)
        metadata['account_uri'] = uri
        metadata['account_name'] = "TEST_acc" + str(accNum)
        metadata['account_tenant_id'] = 'NULL'
        metadata['account_first_use_time'] = self.t
        metadata['account_last_modified_time'] = self.t
        metadata['account_last_changed_time'] = self.t
        metadata['account_delete_time']  = self.t
        metadata['account_last_activity_time']  = self.t
        metadata['account_container_count'] = "1"
        metadata['account_object_count'] = "33"
        metadata['account_bytes_used'] = "3342"
        return metadata

if __name__ == '__main__':
    unittest.main()