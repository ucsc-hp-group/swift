
import errno
import os
import mock
import unittest
from shutil import rmtree
from StringIO import StringIO

import simplejson
import xml.dom.minidom

from swift.common.swob import Request
# from swift.account.server import AccountController, ACCOUNT_LISTING_LIMIT
from swift.metadata.serve import MetaDataController

from swift.common.utils import normalize_timestamp, replication, public
from swift.common.request_helpers import get_sys_meta_prefix


class TestMetaDataController(unittest.TestCase):

    def setUp(self):
        self.testDir = os.path.join(os.path.dirname(__file__), 
            'metadata_controller')
        self.controller = MetaDataController({
                'devices': self.testDir,
                'mount_check': False
            })

    def tearDown(self):
        try:
            rmtree(self.testDir)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise

    '''
    GET request tests
    '''

    '''
    POST request tests
    '''