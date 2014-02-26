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
from swift.container import server as container_server
from swift.container.backend import ContainerBroker
from swift.common.utils import get_logger, audit_location_generator, \
    config_true_value, json
from swift.common.daemon import Daemon
from eventlet import Timeout


class ContainerCrawler(Daemon):
    """Crawls Containers for metadata."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='container-crawler')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.interval = int(conf.get('interval', 120))

        #swift.common.db.DB_PREALLOCATION = \
        #config_true_value(conf.get('db_preallocation', 'f'))
        self.crawled_time = time.time()  # last time this daemon ran

    def _one_crawler_pass(self):
        all_locs = audit_location_generator(self.devices,
                                            container_server.DATADIR, '.db',
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        metaList = []
        for path, device, partition in all_locs:
            metaDict = self.container_crawl(path)
            if metaDict != {}:
                metaList.append(metaDict)
        with open("/opt/stack/data/swift/logs/con-crawler.log", "a+") as f:
            f.write(json.dumps(metaList))
        #sender.send(metaList)

    def run_forever(self, *args, **kwargs):
        """Run the container crawler until stopped."""
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
        """Run the container crawler once."""
        self._one_crawler_pass()

    def container_crawl(self, path):
        """
        Crawls the given container path.

        :param path: the path to an container db
        """
        metaDict = {}
        try:
            broker = ContainerBroker(path)
            if not broker.is_deleted():
                #reportedTime = broker.get_info()['put_timestamp']
                #if normalize_timestamp(self.crawled_time)
                #< reportedTime < normalize_timestamp(start_time):
                metaDict = broker.get_info()
        except (Exception, Timeout):
            self.logger.increment('failures')
        return metaDict
