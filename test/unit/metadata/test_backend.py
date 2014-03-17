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

    '''

    def test_creation(self):

    def test_exception(self):

    def test_empty(self):

    def test_query_with_attrs(self):

    def test_query_with_attrs_and_md_queries(self):

    '''