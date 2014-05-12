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

import time
from random import random
from swift.common.utils import get_logger, config_true_value, normalize_timestamp
from swift.common.daemon import Daemon
from swift.obj.diskfile import DiskFileManager, DiskFileNotExist
from eventlet import Timeout
from swift.common.SendData import Sender


class ObjectCrawler(Daemon):
    """Update object information in container listings."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='object-updater')
        self.devices = conf.get('devices', '/srv/node')
        self.ip = conf.get('md-server-ip', '127.0.0.1')
        self.port = conf.get('md-server-port', '6090')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.interval = int(conf.get('interval', 30))
        #self.container_ring = None
        #self.concurrency = int(conf.get('concurrency', 1))
        #self.slowdown = float(conf.get('slowdown', 0.01))
        #self.node_timeout = int(conf.get('node_timeout', 10))
        #self.conn_timeout = float(conf.get('conn_timeout', .5))
        self.last_time_ran = 0
        self.diskfile_mgr = DiskFileManager(conf, self.logger)

    def run_forever(self, *args, **kwargs):
        """Run the updater continuously."""
        time.sleep(random() * self.interval)
        while True:
            with open("/opt/swift/OBJLOG.txt", "a+") as f:
                f.write("START\n")
            try:
                self.object_sweep()
                self.last_time_ran = time.time()
            except (Exception, Timeout):
                pass
            time.sleep(self.interval)



    def run_once(self, *args, **kwargs):
        """Run the updater once."""
        self.object_sweep()

    def object_sweep(self):
        """
        Scan through all objects and send metadata dict of ones with updates.
        """

        all_locs = self.diskfile_mgr.object_audit_location_generator()
        metaList = []
        for location in all_locs:
            try:
                metaDict = self.collect_object(location)

                metaDict = self.format_metadata(metaDict)
                if metaDict != {}:
                    modtime = metaDict["object_last_modified_time"]
                    if modtime != 'NULL' and float(modtime) > self.last_time_ran:
                        metaList.append(metaDict)
            except Exception:
                pass
            
        if metaList != []:
            ObjectSender = Sender(self.conf)
            ObjectSender.sendData(
                metaList, 'object_crawler', self.ip, self.port)

    def collect_object(self, location):
        """
        Process the object metadata
        """
        metadata = {}
        try:
            df = self.diskfile_mgr.get_diskfile_from_audit_location(location)
            metadata = df.read_metadata()
        except DiskFileNotExist:
            pass
        return metadata

    def format_metadata(self, data):
        metadata = {}
        uri = data['name'].split("/")
        metadata['object_uri'] = data['name']
        metadata['object_name'] = ("/".join(uri[3:]))
        metadata['object_account_name'] = uri[1]
        metadata['object_container_name'] = uri[2]
        metadata['object_location'] = 'NULL'  # Not implemented yet
        metadata['object_uri_create_time'] = \
            data.setdefault('X-Timestamp', 'NULL')

        metadata['object_last_modified_time'] = \
            data.setdefault('X-Timestamp', 'NULL')

        metadata['object_last_changed_time'] = 'NULL'

        metadata['object_delete_time'] = 'NULL'

        metadata['object_last_activity_time'] = \
            data.setdefault('X-Timestamp', 'NULL')

        metadata['object_etag_hash'] = \
            data.setdefault('ETag', 'NULL')

        metadata['object_content_type'] = \
            data.setdefault('Content-Type', 'NULL')

        metadata['object_content_length'] = \
            data.setdefault('Content-Length', 'NULL')

        metadata['object_content_encoding'] = \
            data.setdefault('Content-Encoding', 'NULL')

        metadata['object_content_disposition'] = \
            data.setdefault('Content-Disposition', 'NULL')

        metadata['object_content_language'] = \
            data.setdefault('Content-Langauge', 'NULL')

        metadata['object_cache_control'] = 'NULL' 

        metadata['object_delete_at'] = \
            data.setdefault('X-Delete-At', 'NULL')

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

        #Insert all Object custom metadata
        for custom in data:
            if(custom.startswith("X-Object-Meta")):
                sanitized_custom = custom[2:13].lower() + custom[13:]
                sanitized_custom = sanitized_custom.replace('-', '_')
                metadata[sanitized_custom] = data[custom]

        return metadata
