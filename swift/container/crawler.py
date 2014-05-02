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
    config_true_value
from swift.common.request_helpers import is_sys_or_user_meta
from swift.common.daemon import Daemon
from eventlet import Timeout
from swift.common.SendData import Sender


class ContainerCrawler(Daemon):
    """Crawls Containers for metadata."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='container-crawler')
        self.devices = conf.get('devices', '/srv/node')
        self.ip = conf.get('md-server-ip', '127.0.0.1')
        self.port = conf.get('md-server-port', '6090')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.interval = int(conf.get('interval', 30))

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
                metaList.append(format_metadata(metaDict))
        ContainerSender = Sender(self.conf)
        ContainerSender.sendData(
            metaList, 'container_crawler', self.ip, self.port)

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
                metaDict.update(
                    (key, value)
                    for key, (value, timestamp) in broker.metadata.iteritems()
                    if value != '' and is_sys_or_user_meta('container', key))
        except (Exception, Timeout):
            self.logger.increment('failures')
        return metaDict


def format_metadata(data):
    metadata = {}
    uri = "/" + data['account'] + "/" + data['container']
    metadata['container_uri'] = uri
    metadata['container_name'] = data['container']
    metadata['container_account_name'] = data['account']
    metadata['container_create_time'] = data.setdefault('created_at', 'NULL')

    metadata['container_delete_time'] = \
        data.setdefault('delete_timestamp', 'NULL')

        #last_activity_time needs to be updated on meta server
    metadata['container_read_permissions'] = 'NULL'  # Not Implemented yet
    metadata['container_write_permissions'] = 'NULL'
    metadata['container_sync_to'] = \
        data.setdefault('x_container_sync_point1', 'NULL')

    metadata['container_sync_key'] = \
        data.setdefault('x_container_sync_point2', 'NULL')

    metadata['container_versions_location'] = 'NULL'
    metadata['container_object_count'] = \
        data.setdefault('object_count', 'NULL')

    metadata['container_bytes_used'] = \
        data.setdefault('bytes_used', 'NULL')

    metadata['container_delete_at'] = \
        data.setdefault('delete_timestamp', 'NULL')

    #Insert all Container custom metadata
    for custom in data:
        if(custom.startswith("X-Container-Meta")):
            sanitized_custom = custom[2:16].lower() + custom[16:]
            sanitized_custom = sanitized_custom.replace('-', '_')
            metadata[sanitized_custom] = data[custom]
    return metadata
