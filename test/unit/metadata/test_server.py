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

import errno
import os
import unittest
import time
from shutil import rmtree

from swift.common.swob import Request

from swift.metadata.server import MetadataController

from swift.common.utils import normalize_timestamp, json

Aattrs = (
    "account_uri,account_name,account_last_activity_time,"
    "account_container_count,account_object_count,account_bytes_used,"
    "account_meta_TESTCUSTOM")
Cattrs = (
    "container_uri,container_name,container_account_name,"
    "container_create_time,container_object_count,container_bytes_used,"
    "container_meta_TESTCUSTOM")
Oattrs = (
    "object_uri,object_name,object_account_name,"
    "object_container_name,object_uri_create_time,"
    "object_etag_hash,object_content_type,"
    "object_content_length,object_content_encoding,"
    "object_content_language,object_meta_TESTCUSTOM")


class TestMetadataController(unittest.TestCase):

    def setUp(self):
        """
        Temp dir is created and location stuff is set up for the controller
        """
        self.testDir = os.path.join(
            os.path.dirname(__file__), 'metadata_controller')
        self.controller = MetadataController(
            {
                'location': self.testDir,
                'db_file': os.path.join(self.testDir, 'meta.db')
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
        metaListO = [self.getTestObjDict(a, c, o)]
        reqO = Request.blank(
            '/', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'}, headers={'user-agent': 'object_crawler'},
            body=json.dumps(metaListO))
        respO = reqO.get_response(self.controller)
        self.assert_(respO.status.startswith('204'))

    def uploadCon(self, a, c):
        metaListC = [self.getTestConDict(a, c)]
        reqC = Request.blank(
            '/', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'},
            headers={'user-agent': 'container_crawler'},
            body=json.dumps(metaListC))
        respC = reqC.get_response(self.controller)
        self.assert_(respC.status.startswith('204'))

    def uploadAcc(self, a):
        metaListA = [self.getTestAccDict(a)]
        reqA = Request.blank(
            '/', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'},
            headers={'user-agent': 'account_crawler'},
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
        self.uploadObj(1, 1, 1)
        self.uploadObj(1, 1, 2)
        self.uploadObj(1, 2, 3)
        self.uploadCon(1, 1)
        self.uploadCon(1, 2)
        self.uploadAcc(1)

    def test_GET_ACCscope_objAttrs_metadata(self):
        """
        In account scope, give me object attrs
        Should return info for all 3 objects
        we uploaded in setup
        """
        attrs = Oattrs
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
        self.assertEquals(
            metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj1')

        self.assertEquals(metaReturned['object_name'], 'TEST_obj1')
        self.assertEquals(metaReturned['object_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['object_container_name'], 'TEST_con1')
        self.assertEquals(metaReturned['object_uri_create_time'], self.t)
        self.assertEquals(metaReturned['object_etag_hash'], '0000000000000000')
        self.assertEquals(metaReturned['object_content_type'], 'text/plain')
        self.assertEquals(str(metaReturned['object_content_length']), '42')
        self.assertEquals(metaReturned['object_content_encoding'], 'gzip')
        self.assertEquals(metaReturned['object_content_language'], 'en')
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'CUSTOM')

        testDict = testList[1]
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj2' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj2']
        self.assertEquals(
            metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj2')

        self.assertEquals(metaReturned['object_name'], 'TEST_obj2')
        self.assertEquals(metaReturned['object_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['object_container_name'], 'TEST_con1')
        self.assertEquals(metaReturned['object_uri_create_time'], self.t)
        self.assertEquals(metaReturned['object_etag_hash'], '0000000000000000')
        self.assertEquals(metaReturned['object_content_type'], 'text/plain')
        self.assertEquals(str(metaReturned['object_content_length']), '42')
        self.assertEquals(metaReturned['object_content_encoding'], 'gzip')
        self.assertEquals(metaReturned['object_content_language'], 'en')
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'CUSTOM')

        testDict = testList[2]
        self.assert_('/TEST_acc1/TEST_con2/TEST_obj3' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con2/TEST_obj3']
        self.assertEquals(
            metaReturned['object_uri'], '/TEST_acc1/TEST_con2/TEST_obj3')

        self.assertEquals(metaReturned['object_name'], 'TEST_obj3')
        self.assertEquals(metaReturned['object_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['object_container_name'], 'TEST_con2')
        self.assertEquals(metaReturned['object_uri_create_time'], self.t)
        self.assertEquals(metaReturned['object_etag_hash'], '0000000000000000')
        self.assertEquals(metaReturned['object_content_type'], 'text/plain')
        self.assertEquals(str(metaReturned['object_content_length']), '42')
        self.assertEquals(metaReturned['object_content_encoding'], 'gzip')
        self.assertEquals(metaReturned['object_content_language'], 'en')
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'CUSTOM')

    def test_GET_CONscope_objAttrs_metadata(self):
        """
        In container scope give me object attrs
        Should give back the 2 objects in container 1
        """
        attrs = Oattrs
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
        self.assertEquals(
            metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj1')

        self.assertEquals(metaReturned['object_name'], 'TEST_obj1')
        self.assertEquals(metaReturned['object_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['object_container_name'], 'TEST_con1')
        self.assertEquals(metaReturned['object_uri_create_time'], self.t)
        self.assertEquals(metaReturned['object_etag_hash'], '0000000000000000')
        self.assertEquals(metaReturned['object_content_type'], 'text/plain')
        self.assertEquals(str(metaReturned['object_content_length']), '42')
        self.assertEquals(metaReturned['object_content_encoding'], 'gzip')
        self.assertEquals(metaReturned['object_content_language'], 'en')
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'CUSTOM')

        testDict = testList[1]
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj2' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj2']
        self.assertEquals(
            metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj2')

        self.assertEquals(metaReturned['object_name'], 'TEST_obj2')
        self.assertEquals(metaReturned['object_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['object_container_name'], 'TEST_con1')
        self.assertEquals(metaReturned['object_uri_create_time'], self.t)
        self.assertEquals(metaReturned['object_etag_hash'], '0000000000000000')
        self.assertEquals(metaReturned['object_content_type'], 'text/plain')
        self.assertEquals(str(metaReturned['object_content_length']), '42')
        self.assertEquals(metaReturned['object_content_encoding'], 'gzip')
        self.assertEquals(metaReturned['object_content_language'], 'en')
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'CUSTOM')

    def test_GET_OBJscope_objAttrs_metadata(self):
        """
        In object scope give me object attrs
        Should give back the object in the path
        """
        attrs = Oattrs
        req2 = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp2 = req2.get_response(self.controller)
        self.assert_(resp2.status.startswith('200'))
        testList = json.loads(resp2.body)
        self.assert_(len(testList) == 1)
        testDict = testList[0]
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj1']
        self.assertEquals(
            metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj1')

        self.assertEquals(metaReturned['object_name'], 'TEST_obj1')
        self.assertEquals(metaReturned['object_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['object_container_name'], 'TEST_con1')
        self.assertEquals(metaReturned['object_uri_create_time'], self.t)
        self.assertEquals(metaReturned['object_etag_hash'], '0000000000000000')
        self.assertEquals(metaReturned['object_content_type'], 'text/plain')
        self.assertEquals(str(metaReturned['object_content_length']), '42')
        self.assertEquals(metaReturned['object_content_encoding'], 'gzip')
        self.assertEquals(metaReturned['object_content_language'], 'en')
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'CUSTOM')

    def test_GET_ACCscope_conAttrs_metadata(self):
        """
        In Account scope, specifiying container attrs.
        We should get back all the containers in that account
        """
        attrs = Cattrs
        req2 = Request.blank(
            '/v1/TEST_acc1', environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp2 = req2.get_response(self.controller)
        self.assert_(resp2.status.startswith('200'))
        testList = json.loads(resp2.body)
        self.assert_(len(testList) == 2)
        testDict = testList[0]
        self.assert_('/TEST_acc1/TEST_con1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1']
        self.assertEquals(
            metaReturned['container_uri'], '/TEST_acc1/TEST_con1')

        self.assertEquals(metaReturned['container_name'], 'TEST_con1')
        self.assertEquals(metaReturned['container_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['container_create_time'], self.t)
        self.assertEquals(metaReturned['container_object_count'], 33)
        self.assertEquals(metaReturned['container_bytes_used'], 3342)

        testDict = testList[1]
        self.assert_('/TEST_acc1/TEST_con2' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con2']
        self.assertEquals(
            metaReturned['container_uri'], '/TEST_acc1/TEST_con2')

        self.assertEquals(metaReturned['container_name'], 'TEST_con2')
        self.assertEquals(metaReturned['container_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['container_create_time'], self.t)
        self.assertEquals(metaReturned['container_object_count'], 33)
        self.assertEquals(metaReturned['container_bytes_used'], 3342)
        self.assertEquals(metaReturned['container_meta_TESTCUSTOM'], 'CUSTOM')

    def test_GET_CONscope_conAttrs_metadata(self):
        """
        In Container scope, specifiying container attrs
        We should get back that container's metadata
        """
        attrs = Cattrs
        req2 = Request.blank(
            '/v1/TEST_acc1/TEST_con1', environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp2 = req2.get_response(self.controller)
        self.assert_(resp2.status.startswith('200'))
        testList = json.loads(resp2.body)
        self.assert_(len(testList) == 1)
        testDict = testList[0]
        self.assert_('/TEST_acc1/TEST_con1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1']
        self.assertEquals(
            metaReturned['container_uri'], '/TEST_acc1/TEST_con1')

        self.assertEquals(metaReturned['container_name'], 'TEST_con1')
        self.assertEquals(metaReturned['container_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['container_create_time'], self.t)
        self.assertEquals(metaReturned['container_object_count'], 33)
        self.assertEquals(metaReturned['container_bytes_used'], 3342)
        self.assertEquals(metaReturned['container_meta_TESTCUSTOM'], 'CUSTOM')

    def test_GET_OBJscope_conAttrs_metadata(self):
        """
        In object scope, specifying container attrs
        We should get back the container that the object
        belongs to
        """
        attrs = Cattrs
        req2 = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp2 = req2.get_response(self.controller)
        self.assert_(resp2.status.startswith('200'))
        testList = json.loads(resp2.body)
        self.assert_(len(testList) == 1)
        testDict = testList[0]
        self.assert_('/TEST_acc1/TEST_con1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1']
        self.assertEquals(
            metaReturned['container_uri'], '/TEST_acc1/TEST_con1')

        self.assertEquals(metaReturned['container_name'], 'TEST_con1')
        self.assertEquals(metaReturned['container_account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['container_create_time'], self.t)
        self.assertEquals(metaReturned['container_object_count'], 33)
        self.assertEquals(metaReturned['container_bytes_used'], 3342)
        self.assertEquals(metaReturned['container_meta_TESTCUSTOM'], 'CUSTOM')

    def test_GET_ACCscope_accAttrs_metadata(self):
        """
        In account scope specifiying account attrs,
        we should get back that account
        """
        attrs = Aattrs
        req2 = Request.blank(
            '/v1/TEST_acc1', environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp2 = req2.get_response(self.controller)
        self.assert_(resp2.status.startswith('200'))
        testList = json.loads(resp2.body)
        self.assert_(len(testList) == 1)
        testDict = testList[0]
        self.assert_('/TEST_acc1' in testDict)
        metaReturned = testDict['/TEST_acc1']
        self.assertEquals(metaReturned['account_uri'], '/TEST_acc1')
        self.assertEquals(metaReturned['account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['account_last_activity_time'], self.t)
        self.assertEquals(metaReturned['account_container_count'], 1)
        self.assertEquals(metaReturned['account_object_count'], 33)
        self.assertEquals(metaReturned['account_bytes_used'], 3342)
        self.assertEquals(metaReturned['account_meta_TESTCUSTOM'], 'CUSTOM')

    def test_GET_CONscope_accAttrs_metadata(self):
        """
        In container scope specifying account attrs
        We should get back the account that the
        container belongs to
        """
        attrs = Aattrs
        req2 = Request.blank(
            '/v1/TEST_acc1/TEST_con1', environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp2 = req2.get_response(self.controller)
        self.assert_(resp2.status.startswith('200'))
        testList = json.loads(resp2.body)
        self.assert_(len(testList) == 1)
        testDict = testList[0]
        self.assert_('/TEST_acc1' in testDict)
        metaReturned = testDict['/TEST_acc1']
        self.assertEquals(metaReturned['account_uri'], '/TEST_acc1')
        self.assertEquals(metaReturned['account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['account_last_activity_time'], self.t)
        self.assertEquals(metaReturned['account_container_count'], 1)
        self.assertEquals(metaReturned['account_object_count'], 33)
        self.assertEquals(metaReturned['account_bytes_used'], 3342)
        self.assertEquals(metaReturned['account_meta_TESTCUSTOM'], 'CUSTOM')

    def test_GET_OBJscope_accAttrs_metadata(self):
        """
        In object scope specifying account attrs
        We should get back the account the object belongs to
        """
        attrs = Aattrs
        req2 = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp2 = req2.get_response(self.controller)
        self.assert_(resp2.status.startswith('200'))
        testList = json.loads(resp2.body)
        self.assert_(len(testList) == 1)
        testDict = testList[0]
        self.assert_('/TEST_acc1' in testDict)
        metaReturned = testDict['/TEST_acc1']
        self.assertEquals(metaReturned['account_uri'], '/TEST_acc1')
        self.assertEquals(metaReturned['account_name'], 'TEST_acc1')
        self.assertEquals(metaReturned['account_last_activity_time'], self.t)
        self.assertEquals(metaReturned['account_container_count'], 1)
        self.assertEquals(metaReturned['account_object_count'], 33)
        self.assertEquals(metaReturned['account_bytes_used'], 3342)
        self.assertEquals(metaReturned['account_meta_TESTCUSTOM'], 'CUSTOM')

    def test_GET_ACCscope_mixedAttrs(self):
        """
        In account scope specifying mixed attributes
        We should get back:
            Attrs from the account
            Attrs from all containers under the account
            Attrs from all objects under the account
        """
        attrs = Aattrs + "," + Cattrs + "," + Oattrs
        req2 = Request.blank(
            '/v1/TEST_acc1', environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp2 = req2.get_response(self.controller)
        self.assert_(resp2.status.startswith('200'))
        testList = json.loads(resp2.body)
        self.assert_(len(testList) == 6)
        self.acc1helper(testList[0], True)
        self.con1helper(testList[1], True)
        self.con2helper(testList[2], True)
        self.obj1helper(testList[3], True)
        self.obj2helper(testList[4], True)
        self.obj3helper(testList[5], True)

    def test_GET_CONscope_mixedAttrs(self):
        """
        In container scope, specifying mixed attrs
        We should get back
            attrs from the account the container belongs to
            attrs from the container specified
            attrs from all the objects under that container
        """
        attrs = Aattrs + "," + Cattrs + "," + Oattrs
        req2 = Request.blank(
            '/v1/TEST_acc1/TEST_con1', environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp2 = req2.get_response(self.controller)
        self.assert_(resp2.status.startswith('200'))
        testList = json.loads(resp2.body)
        self.assert_(len(testList) == 4)

        self.acc1helper(testList[0], True)
        self.con1helper(testList[1], True)
        self.obj1helper(testList[2], True)
        self.obj2helper(testList[3], True)

    def test_GET_OBJscope_mixedAttrs(self):
        """
        In object scope, mixed attrs
        Should get back
            attrs from the account the object is under
            attrs from the container the object is under
            attrs from tyhe object
        """
        attrs = Aattrs + "," + Cattrs + "," + Oattrs
        req2 = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp2 = req2.get_response(self.controller)
        self.assert_(resp2.status.startswith('200'))
        testList = json.loads(resp2.body)
        self.assert_(len(testList) == 3)

        self.acc1helper(testList[0], True)
        self.con1helper(testList[1], True)
        self.obj1helper(testList[2], True)

    def test_superset_all_attrs_acc_scope(self):
        """
        Tests all_attrs
        Gets everything in account's scope
        """
        attrs = "all_attrs"
        req = Request.blank(
            '/v1/TEST_acc1', environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 6)

        self.acc1helper(testList[0], True)
        self.con1helper(testList[1], True)
        self.con2helper(testList[2], True)
        self.obj1helper(testList[3], True)
        self.obj2helper(testList[4], True)
        self.obj3helper(testList[5], True)

    def test_superset_all_attrs_con_scope(self):
        """
        We should get back:
            attrs from acc of con, the con, and all objects in con
        """
        attrs = "all_attrs"
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1', environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 4)

        self.acc1helper(testList[0], True)
        self.con1helper(testList[1], True)
        self.obj1helper(testList[2], True)
        self.obj2helper(testList[3], True)

    def test_superset_all_attrs_obj_scope(self):
        """
        We should get back:
            attrs from the obj's acc, the obj's con, the obj
        """
        attrs = "all_attrs"
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 3)

        self.acc1helper(testList[0], True)
        self.con1helper(testList[1], True)
        self.obj1helper(testList[2], True)

    def test_superset_all_system_attrs(self):
        attrs = "all_system_attrs"
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 3)

        self.acc1helper(testList[0], False)
        self.con1helper(testList[1], False)
        self.obj1helper(testList[2], False)

    def test_superset_all_meta_attrs(self):
        attrs = "all_meta_attrs"
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 3)

        self.acc1helperCustom(testList[0])
        self.con1helperCustom(testList[1])
        self.obj1helperCustom(testList[2])

    def test_superset_all_account_attrs(self):
        attrs = "all_account_attrs"
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 1)

        self.acc1helper(testList[0], True)

    def test_superset_all_account_system_attrs(self):
        attrs = "all_account_system_attrs"
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 1)

        self.acc1helper(testList[0], False)

    def test_superset_all_account_meta_attrs(self):
        attrs = "all_account_meta_attrs"
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 1)

        self.acc1helperCustom(testList[0])

    def test_superset_all_container_attrs(self):
        attrs = "all_container_attrs"
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 1)

        self.con1helper(testList[0], True)

    def test_superset_all_container_system_attrs(self):
        attrs = "all_container_system_attrs"
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 1)

        self.con1helper(testList[0], False)

    def test_superset_all_container_meta_attrs(self):
        attrs = "all_container_meta_attrs"
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 1)

        self.con1helperCustom(testList[0])

    def test_superset_all_object_attrs(self):
        attrs = "all_object_attrs"
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 1)

        self.obj1helper(testList[0], True)

    def test_superset_all_object_system_attrs(self):
        attrs = "all_object_system_attrs"
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 1)

        self.obj1helper(testList[0], False)

    def test_superset_all_object_meta_attrs(self):
        attrs = "all_object_meta_attrs"
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 1)

        self.obj1helperCustom(testList[0])


    def test_no_attributes_in_request_obj_scope(self):
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 3)
        testDict = testList[0]
        self.assert_('/TEST_acc1' in testDict)
        metaReturned = testDict['/TEST_acc1']
        self.assertEquals(metaReturned['account_uri'], '/TEST_acc1')

        testDict = testList[1]
        self.assert_('/TEST_acc1/TEST_con1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1']
        self.assertEquals(metaReturned['container_uri'], '/TEST_acc1/TEST_con1')

        testDict = testList[2]
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj1']
        self.assertEquals(metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj1')

    def test_no_attributes_in_request_con_scope(self):
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 4)

        testDict = testList[0]
        self.assert_('/TEST_acc1' in testDict)
        metaReturned = testDict['/TEST_acc1']
        self.assertEquals(metaReturned['account_uri'], '/TEST_acc1')

        testDict = testList[1]
        self.assert_('/TEST_acc1/TEST_con1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1']
        self.assertEquals(metaReturned['container_uri'], '/TEST_acc1/TEST_con1')

        testDict = testList[2]
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj1']
        self.assertEquals(metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj1')

        testDict = testList[3]
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj2' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj2']
        self.assertEquals(metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj2')

    def test_no_attributes_in_request_acc_scope(self):
        req = Request.blank(
            '/v1/TEST_acc1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('200'))
        testList = json.loads(resp.body)
        self.assertEquals(len(testList), 6)

        testDict = testList[0]
        self.assert_('/TEST_acc1' in testDict)
        metaReturned = testDict['/TEST_acc1']
        self.assertEquals(metaReturned['account_uri'], '/TEST_acc1')

        testDict = testList[1]
        self.assert_('/TEST_acc1/TEST_con1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1']
        self.assertEquals(metaReturned['container_uri'], '/TEST_acc1/TEST_con1')

        testDict = testList[2]
        self.assert_('/TEST_acc1/TEST_con2' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con2']
        self.assertEquals(metaReturned['container_uri'], '/TEST_acc1/TEST_con2')

        testDict = testList[3]
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj1']
        self.assertEquals(metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj1')

        testDict = testList[4]
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj2' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj2']
        self.assertEquals(metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj2')

        testDict = testList[5]
        self.assert_('/TEST_acc1/TEST_con2/TEST_obj3' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con2/TEST_obj3']
        self.assertEquals(metaReturned['object_uri'], '/TEST_acc1/TEST_con2/TEST_obj3')

    def test_bad_attrs(self):
        attrs = "bad_attr"
        req = Request.blank(
            '/v1/TEST_acc1/TEST_con1/TEST_obj1',
            environ={'REQUEST_METHOD': 'GET',
            'HTTP_X_TIMESTAMP': '0'}, headers={'attributes': attrs})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('400'))

    ########################
    #   HELPER FUNCTIONS   #
    ########################

    """
    For each of these acc/con/obj helper functions, we give the dictionary
    to test and wether we want to test custom metadata or not.
    Basically test wether the dictionary had the URI as key, and
    then test if a system attribute is there and if we want
    to test custom, then call a seperate function to do that
    There needs to be seperate functions because we need to
    test only custom attributes
    """

    def acc1helper(self, testDict, custom):
        self.assert_('/TEST_acc1' in testDict)
        metaReturned = testDict['/TEST_acc1']
        self.assertEquals(metaReturned['account_uri'], '/TEST_acc1')
        self.assertEquals(metaReturned['account_bytes_used'], 3342)
        if custom:
            self.acc1helperCustom(testDict)

    def acc1helperCustom(self, testDict):
        self.assert_('/TEST_acc1' in testDict)
        metaReturned = testDict['/TEST_acc1']
        self.assertEquals(metaReturned['account_meta_TESTCUSTOM'], 'CUSTOM')

    def con1helper(self, testDict, custom):
        self.assert_('/TEST_acc1/TEST_con1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1']
        self.assertEquals(
            metaReturned['container_uri'], '/TEST_acc1/TEST_con1')

        self.assertEquals(metaReturned['container_bytes_used'], 3342)
        if custom:
            self.con1helperCustom(testDict)

    def con1helperCustom(self, testDict):
        self.assert_('/TEST_acc1/TEST_con1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1']
        self.assertEquals(metaReturned['container_meta_TESTCUSTOM'], 'CUSTOM')

    def con2helper(self, testDict, custom):
        self.assert_('/TEST_acc1/TEST_con2' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con2']
        self.assertEquals(
            metaReturned['container_uri'], '/TEST_acc1/TEST_con2')

        self.assertEquals(metaReturned['container_bytes_used'], 3342)
        if custom:
            self.con2helperCustom(testDict)

    def con2helperCustom(self, testDict):
        self.assert_('/TEST_acc1/TEST_con2' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con2']
        self.assertEquals(metaReturned['container_meta_TESTCUSTOM'], 'CUSTOM')

    def obj1helper(self, testDict, custom):
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj1']
        self.assertEquals(
            metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj1')

        self.assertEquals(metaReturned['object_content_language'], 'en')
        if custom:
            self.obj1helperCustom(testDict)

    def obj1helperCustom(self, testDict):
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj1' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj1']
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'CUSTOM')

    def obj2helper(self, testDict, custom):
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj2' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj2']
        self.assertEquals(
            metaReturned['object_uri'], '/TEST_acc1/TEST_con1/TEST_obj2')

        self.assertEquals(metaReturned['object_content_language'], 'en')
        if custom:
            self.obj2helperCustom(testDict)

    def obj2helperCustom(self, testDict):
        self.assert_('/TEST_acc1/TEST_con1/TEST_obj2' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con1/TEST_obj2']
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'CUSTOM')

    def obj3helper(self, testDict, custom):
        self.assert_('/TEST_acc1/TEST_con2/TEST_obj3' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con2/TEST_obj3']
        self.assertEquals(
            metaReturned['object_uri'], '/TEST_acc1/TEST_con2/TEST_obj3')

        self.assertEquals(metaReturned['object_content_language'], 'en')
        if custom:
            self.obj3helpercustom(testDict)

    def obj3helpercustom(self, testDict):
        self.assert_('/TEST_acc1/TEST_con2/TEST_obj3' in testDict)
        metaReturned = testDict['/TEST_acc1/TEST_con2/TEST_obj3']
        self.assertEquals(metaReturned['object_meta_TESTCUSTOM'], 'CUSTOM')

    """
    Create test dictionaries for putting into the test metadata db
    """
    def getTestObjDict(self, accNum, conNum, objNum):
        metadata = {}
        uri = "/TEST_acc" + str(accNum) + "/TEST_con" + str(conNum) + \
            "/TEST_obj" + str(objNum)
        uri_list = uri.split("/")
        metadata['object_uri'] = uri
        metadata['object_name'] = ("/".join(uri_list[3:]))
        metadata['object_account_name'] = uri_list[1]
        metadata['object_container_name'] = uri_list[2]
        metadata['object_location'] = 'NULL'  # Not implemented yet
        metadata['object_uri_create_time'] = self.t
        # Uri create needs to be implemented on meta server.
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
        metadata['object_cache_control'] = 'NULL'  # Not Implemented yet
        metadata['object_delete_at'] = self.t
        metadata['object_manifest_type'] = 'NULL'
        metadata['object_manifest'] = 'NULL'
        metadata['object_access_control_allow_origin'] = 'NULL'
        metadata['object_access_control_allow_credentials'] = 'NULL'
        metadata['object_access_control_expose_headers'] = 'NULL'
        metadata['object_access_control_max_age'] = 'NULL'
        metadata['object_access_control_allow_methods'] = 'NULL'
        metadata['object_access_control_allow_headers'] = 'NULL'
        metadata['object_origin'] = 'NULL'
        metadata['object_access_control_request_method'] = 'NULL'
        metadata['object_access_control_request_headers'] = 'NULL'
        metadata['object_meta_TESTCUSTOM'] = 'CUSTOM'
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
        metadata['container_read_permissions'] = 'NULL'  # Not Implemented yet
        metadata['container_write_permissions'] = 'NULL'
        metadata['container_sync_to'] = 'NULL'
        metadata['container_sync_key'] = 'NULL'
        metadata['container_versions_location'] = 'NULL'
        metadata['container_object_count'] = '33'
        metadata['container_bytes_used'] = '3342'
        metadata['container_delete_at'] = self.t
        metadata['container_meta_TESTCUSTOM'] = 'CUSTOM'
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
        metadata['account_delete_time'] = self.t
        metadata['account_last_activity_time'] = self.t
        metadata['account_container_count'] = "1"
        metadata['account_object_count'] = "33"
        metadata['account_bytes_used'] = "3342"
        metadata['account_meta_TESTCUSTOM'] = 'CUSTOM'
        return metadata

if __name__ == '__main__':
    unittest.main()
