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
import unittest
from contextlib import closing
from shutil import rmtree
from tempfile import mkdtemp

from eventlet import spawn, Timeout, listen

from swift.common import utils
from swift.container import crawler as container_crawler
from swift.container.backend import ContainerBroker
from swift.common.utils import normalize_timestamp
from swift.container import server as container_server


class FakeContainerBroker(object):
    def __init__(self, path):
        self.path = path
        self.db_file = path
        self.file = os.path.basename(path)

    def is_deleted(self):
        return False

    def get_info(self):
        if self.file.startswith('fail'):
            raise ValueError
        if self.file.startswith('true'):
            return 'ok'


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
        cc = container_crawler.ContainerCrawler({
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

if __name__ == '__main__':
    unittest.main()
