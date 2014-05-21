# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# import cPickle as pickle
import os
import unittest
# from contextlib import closing
from shutil import rmtree
from tempfile import mkdtemp

# from eventlet import spawn, Timeout, listen

# from swift.common import utils
from swift.container import crawler
from swift.container.backend import ContainerBroker
from swift.common.utils import normalize_timestamp
from swift.container import server as container_server



# class FakeContainerBroker(object):
#     def __init__(self, path):
#         self.path = path
#         self.db_file = path
#         self.file = os.path.basename(path)

#     def is_deleted(self):
#         return False

#     def get_info(self):
#         if self.file.startswith('fail'):
#             raise ValueError
#         if self.file.startswith('true'):
#             return 'ok'


class TestAuditor(unittest.TestCase):

    def setUp(self):
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_container_updater')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        self.devices_dir = os.path.join(self.testdir, 'devices')
        os.mkdir(self.devices_dir)
        self.sda1 = os.path.join(self.devices_dir, 'sda1')
        os.mkdir(self.sda1)

        containers_dir = os.path.join(self.sda1, container_server.DATADIR)
        os.mkdir(containers_dir)

        self.assert_(os.path.exists(containers_dir))
        self.subdir = os.path.join(containers_dir, 'subdir')
        os.mkdir(self.subdir)
        cb = ContainerBroker(os.path.join(self.subdir, 'hash.db'), account='a',
                             container='c')
        cb.initialize(normalize_timestamp(1))
        cb.put_object('o', normalize_timestamp(2), 3, 'text/plain',
                '68b329da9893e34099c7d8ad5cb9c940')

    def tearDown(self):
        rmtree(os.path.dirname(self.testdir), ignore_errors=1)

    def test_container_crawl(self):
        cc = crawler.ContainerCrawler({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'account_suppression_time': 0
        })

        metaDict = cc.container_crawl(os.path.join(self.subdir, 'hash.db'))
        self.assertEquals(metaDict['object_count'], 1)
        self.assertEquals(metaDict['account'], 'a')
        self.assertEquals(metaDict['container'], 'c')
        self.assertEquals(metaDict['put_timestamp'], normalize_timestamp(1))
        self.assertEquals(metaDict['bytes_used'], 3)
        self.assertEquals(metaDict['x_container_sync_point1'], -1)
        self.assertEquals(metaDict['x_container_sync_point2'], -1)

    def test_format_metadata(self):
        inputdata = {'object_count': 1,
            'account': 'AUTH_admin',
            'container': 'testCon1',
            'reported_object_count': 1,
            'reported_delete_timestamp': '0',
            'bytes_used': 39388,
            'created_at': '1399924386.49468',
            'put_timestamp': '1399924386.48496',
            'x_container_sync_point2': -1,
            'x_container_sync_point1': -1,
            'id': 'ad6ed2d8-21ee-4c46-b363-ea4e18371439',
            'delete_timestamp': '0',
            'reported_put_timestamp': '1399924386.48496',
            'hash': '8e49ea64679c0daa264be235fdbff99a',
            'reported_bytes_used': 39388
        }
        outputexpected = {
            'container_sync_key': -1,
            'container_last_changed_time': '1399924386.48496',
            'container_create_time': '1399924386.49468',
            'container_versions_location': 'NULL',
            'container_uri': '/AUTH_admin/testCon1',
            'container_last_modified_time': '1399924386.48496',
            'container_account_name': 'AUTH_admin',
            'container_write_permissions': 'NULL',
            'container_delete_time': '0',
            'container_object_count': 1,
            'container_name': 'testCon1',
            'container_delete_at': '0',
            'container_bytes_used': 39388,
            'container_last_activity_time': '1399924386.48496',
            'container_sync_to': -1,
            'container_read_permissions': 'NULL'
        }

        outputdata = crawler.format_metadata(inputdata)
        self.assertEquals(outputexpected['container_sync_key'], outputdata['container_sync_key'])
        self.assertEquals(outputexpected['container_last_changed_time'], outputdata['container_last_changed_time'])
        self.assertEquals(outputexpected['container_create_time'], outputdata['container_create_time'])
        self.assertEquals(outputexpected['container_versions_location'], outputdata['container_versions_location'])
        self.assertEquals(outputexpected['container_uri'], outputdata['container_uri'])
        self.assertEquals(outputexpected['container_last_modified_time'], outputdata['container_last_modified_time'])
        self.assertEquals(outputexpected['container_account_name'], outputdata['container_account_name'])
        self.assertEquals(outputexpected['container_write_permissions'], outputdata['container_write_permissions'])
        self.assertEquals(outputexpected['container_delete_time'], outputdata['container_delete_time'])
        self.assertEquals(outputexpected['container_bytes_used'], outputdata['container_bytes_used'])
        self.assertEquals(outputexpected['container_last_activity_time'], outputdata['container_last_activity_time'])
        self.assertEquals(outputexpected['container_sync_to'], outputdata['container_sync_to'])
        self.assertEquals(outputexpected['container_read_permissions'], outputdata['container_read_permissions'])

if __name__ == '__main__':
    unittest.main()
