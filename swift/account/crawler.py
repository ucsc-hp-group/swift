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

import os
import time
from swift import gettext_ as _
from random import random

import swift.common.db
from swift.account import server as account_server
from swift.account.backend import AccountBroker
from swift.common.utils import get_logger, audit_location_generator, \
    config_true_value, dump_recon_cache, ratelimit_sleep, normalize_timestamp
from swift.common.daemon import Daemon
import json
from eventlet import Timeout


class AccountCrawler(Daemon):
    """Crawls accounts for metadata."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='account-crawler')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.interval = int(conf.get('interval', 120))
        self.account_passes = 0
        self.account_failures = 0
        self.accounts_running_time = 0
        self.max_accounts_per_second = \
            float(conf.get('accounts_per_second', 200))
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "account.recon")
        self.crawled_time = time.time() # last time this daemon ran

    def _one_crawler_pass(self, reported):
        all_locs = audit_location_generator(self.devices,
                                            account_server.DATADIR, '.db',
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        metaList = []
        for path, device, partition in all_locs:
            metaDict = self.account_crawl(path)
            if metaDict != {}:
                metaList.append(metaDict)
            if time.time() - reported >= 3600: # once an hour
                self.logger.info(_('Since %(time)s: Account crawls: '
                                   '%(passed)s passed crawl,'
                                   '%(failed)s failed crawl'),
                                 {'time': time.ctime(reported),
                                  'passed': self.account_passes,
                                  'failed': self.account_failures})
                reported = time.time()
                self.account_passes = 0
                self.account_failures = 0
            self.accounts_running_time = ratelimit_sleep(
                self.accounts_running_time, self.max_accounts_per_second)
        f = open("/opt/stack/data/swift/logs/acc-crawler.log", "w+")
        f.write(json.dumps(metaList))
        f.close()
        #sender.send(metaList)
        return reported

    def run_forever(self, *args, **kwargs):
        """Run the account crawler until stopped."""
        reported = time.time()
        time.sleep(random() * self.interval)
        while True:
            self.logger.info(_('Begin account crawler pass.'))
            begin = time.time()
            try:
                reported = self._one_crawler_pass(reported)
            except (Exception, Timeout):
                self.logger.increment('errors')
                self.logger.exception(_('ERROR crawling'))
            elapsed = time.time() - begin
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self.logger.info(
                _('Account crawl pass completed: %.02fs'), elapsed)

    def run_once(self, *args, **kwargs):
        """Run the account crawler once."""
        self.logger.info(_('Begin account crawler "once" mode'))
        begin = reported = time.time()
        self._one_crawler_pass(reported)
        elapsed = time.time() - begin
        self.logger.info(
            _('Account crawler "once" mode completed: %.02fs'), elapsed)

    def account_crawl(self, path):
        """
Crawls the given account path

:param path: the path to an account db
"""
        start_time = time.time()
        metaDict = {}
        try:
            broker = AccountBroker(path)
            if not broker.is_deleted():
                reportedTime = broker.get_info()['put_timestamp']
                #if normalize_timestamp(self.crawled_time) < reportedTime < normalize_timestamp(start_time):
                metaDict = broker.get_info()
                self.logger.increment('passes')
                self.account_passes += 1
                self.logger.debug(_('Metadata Crawler passed for %s') % broker)
        except (Exception, Timeout):
            self.logger.increment('failures')
            self.account_failures += 1
            self.logger.exception(_('ERROR Could not get account info %s'),
                                  path)
        self.logger.timing_since('timing', start_time)
        return metaDict



