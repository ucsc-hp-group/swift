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

import time
from random import random
from swift.account import server as account_server
from swift.account.backend import AccountBroker
from swift.common.utils import get_logger, audit_location_generator, \
    config_true_value
from swift.common.daemon import Daemon
from eventlet import Timeout
from swift.metadata.utils import Sender


class AccountCrawler(Daemon):
    """Crawls accounts for metadata."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='account-crawler')
        self.devices = conf.get('devices', '/srv/node')
        self.ip = conf.get('md-server-ip', '127.0.0.1')
        self.port = conf.get('md-server-port', '6090')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.interval = int(conf.get('interval', 30))
        self.crawled_time = time.time()  # last time this daemon ran

    def _one_crawler_pass(self):
        all_locs = audit_location_generator(self.devices,
                                            account_server.DATADIR, '.db',
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        metaList = []
        for path, device, partition in all_locs:
            metaDict = self.account_crawl(path)
            if metaDict != {}:
                metaList.append(format_metadata(metaDict))
        AccountSender = Sender(self.conf)
        AccountSender.sendData(metaList, 'account_crawler', self.ip, self.port)

    def run_forever(self, *args, **kwargs):
        """Run the account crawler until stopped."""
        time.sleep(random() * self.interval)
        while True:
            begin = time.time()
            try:
                self._one_crawler_pass()
            except (Exception, Timeout):
                self.logger.increment('errors')
            elapsed = time.time() - begin
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

    def run_once(self, *args, **kwargs):
        """Run the account crawler once."""
        self._one_crawler_pass()

    def account_crawl(self, path):
        """
        Crawls the given account path

        :param path: the path to an account db
        """
        #start_time = time.time()
        metaDict = {}
        try:
            broker = AccountBroker(path)
            if not broker.is_deleted():
                #reportedTime = broker.get_info()['put_timestamp']
                #if normalize_timestamp(self.crawled_time) <
                #reportedTime < normalize_timestamp(start_time):
                metaDict = broker.get_info()
                metaDict.update((key, value)
                       for key, (value, timestamp) in
                       broker.metadata.iteritems() if value != '')
        except (Exception, Timeout):
            self.logger.increment('failures')
        return metaDict


def format_metadata(data):
    metadata = {}
    uri = "/" + data['account']
    metadata['account_uri'] = uri
    metadata['account_name'] = data['account']
    metadata['account_tenant_id'] = data.setdefault('id', 'NULL')
    metadata['account_first_use_time'] = data.setdefault('created_at', 'NULL')
    metadata['account_last_modified_time'] = \
        data.setdefault('put_timestamp', 'NULL')

    metadata['account_last_changed_time'] =  \
        data.setdefault('put_timestamp', 'NULL')

    metadata['account_delete_time'] = \
        data.setdefault('delete_timestamp', 'NULL')

    metadata['account_last_activity_time'] = \
        data.setdefault('put_timestamp', 'NULL')

    metadata['account_container_count'] = \
        data.setdefault('container_count', 'NULL')

    metadata['account_object_count'] = \
        data.setdefault('object_count', 'NULL')

    metadata['account_bytes_used'] = data.setdefault('bytes_used', 'NULL')

    #Insert all Account custom metadata
    for custom in data:
        if(custom.lower().startswith("x-account-meta")):
            sanitized_custom = custom[2:14].lower() + custom[14:]
            sanitized_custom = sanitized_custom.replace('-', '_')
            metadata[sanitized_custom] = data[custom]
    return metadata
