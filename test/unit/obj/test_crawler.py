#-*- coding:utf-8 -*-
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

import cPickle as pickle
import os
import errno
import mock
import unittest
import email
import tempfile
import xattr
from shutil import rmtree
from time import time
from tempfile import mkdtemp
from hashlib import md5
from contextlib import closing, nested
from gzip import GzipFile

from eventlet import tpool
from test.unit import FakeLogger, mock as unit_mock, temptree

from swift.obj import diskfile
from swift.obj import crawler
from swift.common import utils
from swift.common.utils import hash_path, mkdirs, normalize_timestamp
from swift.common import ring
from swift.common.exceptions import DiskFileNotExist, DiskFileQuarantined, \
    DiskFileDeviceUnavailable, DiskFileDeleted, DiskFileNotOpen, \
    DiskFileError, ReplicationLockTimeout, PathNotDir, DiskFileCollision, \
    DiskFileExpired, SwiftException, DiskFileNoSpace

"""Tests for swift.obj.crawler"""


class TestObjectCrawler(unittest.TestCase):

    def setUp(self):
        self.tmpdir = mkdtemp()
        self.testdir = os.path.join(
                self.tmpdir, 'tmp_test_obj_server_DiskFile')
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))
        self._orig_tpool_exc = tpool.execute
        tpool.execute = lambda f, *args, **kwargs: f(*args, **kwargs)
        self.conf = dict(devices=self.testdir, mount_check='false',
                         keep_cache_size=2 * 1024, mb_per_sync=1)
        self.df_mgr = diskfile.DiskFileManager(self.conf, FakeLogger())
        self.crawler = crawler.ObjectCrawler(self.conf)

    def _create_test_file(self, data, timestamp=None, metadata=None,
                          account='a', container='c', obj='o'):
        if metadata is None:
            metadata = {}
        metadata.setdefault('name', '/%s/%s/%s' % (account, container, obj))
        df = self.df_mgr.get_diskfile('sda', '1', account, container, obj)
        self._create_ondisk_file(df, data, timestamp, metadata)
        #df = self.df_mgr.get_diskfile('sda', '0', account, container, obj)
        df.open()
        return df

    def _create_ondisk_file(self, df, data, timestamp, metadata=None,
                            ext='.data'):
        mkdirs(df._datadir)
        if timestamp is None:
            timestamp = time()
            timestamp = normalize_timestamp(timestamp)
        data_file = os.path.join(df._datadir, timestamp + ext)
        with open(data_file, 'wb') as f:
            f.write(data)
            xattr.setxattr(f.fileno(), diskfile.METADATA_KEY,
                           pickle.dumps(metadata, diskfile.PICKLE_PROTOCOL))


    def test_collect_object(self):
        t = 42
        t = normalize_timestamp(t)
        data = 'ContentHere'
        etag = md5()
        etag.update(data)
        testdir = self._create_test_file(
            data,
            timestamp = t, 
            account='TEST_Acc', container='TEST_Con', obj='TEST_Obj',
            metadata={
                     'X-Timestamp':t,
                     'ETag' : etag.hexdigest(),
                     'Content-Length' : str(len(data)),
                     'delete_timestamp' : normalize_timestamp(0),
                     'Content-Type' : "text/plain",
                     'Content-Encoding' : 'gzip',
                     'Content-Disposition' : 'action',
                     'Content-Langauge':'en'
            })._datadir
        location = diskfile.AuditLocation(testdir, 'sda1', '0')
        metaDict = self.crawler.collect_object(location)
        self.assertEquals(metaDict['name'],'/TEST_Acc/TEST_Con/TEST_Obj')
        self.assertEquals(metaDict['X-Timestamp'],t)
        self.assertEquals(metaDict['ETag'],etag.hexdigest())
        self.assertEquals(metaDict['Content-Length'],str(len(data)))
        self.assertEquals(metaDict['Content-Type'],'text/plain')
        self.assertEquals(metaDict['Content-Encoding'],'gzip')
        self.assertEquals(metaDict['Content-Disposition'],'action')
        self.assertEquals(metaDict['Content-Langauge'],'en')


    def test_format_metadata(self):
        formattedmetadata = self.crawler.format_metadata({'name':'/TEST_Acc/TEST_Con/TEST_Obj'})
        self.assertEquals(formattedmetadata['object_uri'],'/TEST_Acc/TEST_Con/TEST_Obj')
        self.assertEquals(formattedmetadata['object_name'],'TEST_Obj')
        self.assertEquals(formattedmetadata['object_account_name'],'TEST_Acc')
        self.assertEquals(formattedmetadata['object_container_name'],'TEST_Con')
        self.assertEquals(formattedmetadata['object_uri_create_time'],'NULL')
        self.assertEquals(formattedmetadata['object_etag_hash'],'NULL')
        self.assertEquals(formattedmetadata['object_content_type'],'NULL')
        self.assertEquals(formattedmetadata['object_content_length'],'NULL')
        # added more tests as more attributes are implemented.

if __name__ == '__main__':
    unittest.main()
