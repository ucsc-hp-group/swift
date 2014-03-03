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
from swift.common.utils import get_logger, config_true_value, json
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
        self.last_time_ran = time.time()
        self.diskfile_mgr = DiskFileManager(conf, self.logger)

    def run_forever(self, *args, **kwargs):
        """Run the updater continuously."""
        time.sleep(random() * self.interval)
        self.last_time_ran = time.time()
        while True:
            try:
                self.object_sweep()
            except (Exception, Timeout):
                with open("/opt/stack/data/swift/logs/obj-crawler.log", "a+") as f:
                    f.write("Exception on object_sweep\n")
            time.sleep(self.interval)

    def run_once(self, *args, **kwargs):
        """Run the updater once."""
        self.object_sweep()

    def object_sweep(self):
        """
        Scan through all objects and return meta data dict
        """

        all_locs = self.diskfile_mgr.object_audit_location_generator()
        metaList = []
        for location in all_locs:
            metaDict = self.collect_object(location)
            if metaDict != {}:
                metaList.append(format_metadata(metaDict))
        ObjectSender = Sender(self.conf)
        resp = ObjectSender.sendData(metaList, 'object_crawler' , self.ip, self.port)
        with open("/opt/stack/data/swift/logs/obj-crawler.log", "a+") as f:
            f.write(resp.read())


    def collect_object(self, location):
        """
        Process the object metadata
        """
        metadata = {}
        try:
            df = self.diskfile_mgr.get_diskfile_from_audit_location(location)
            metadata = df.read_metadata()
        except DiskFileNotExist:
            with open("/opt/stack/data/swift/logs/obj-crawler.log", "a+") as f:
                f.write("DISKFILE DOES NOT EXIST\n")
        return metadata

def format_metadata (data):
    metadata = {}
    uri = data['name'].split("/") 
    metadata['object_uri'] = data['name']
    metadata['object_name'] = ("/".join(uri[3:]))
    metadata['object_account_name'] = uri[1]
    metadata['object_container_name'] = uri[2]
    metadata['object_location'] = data.setDefault('object_location','NULL')
    metadata['object_uri_create_time'] = data.setDefault('object_uri_create_time','NULL')
    metadata['object_last_modified_time'] = data.setDefault('object_last_modified_time','NULL')
    metadata['object_last_changed_time'] = data.setDefault('object_last_changed_time','NULL')
    metadata['object_delete_time'] = data.setDefault('delete_time','NULL')
    metadata['object_last_activity_time'] = data.setDefault('Last_activity_time','NULL')
    metadata['object_etag_hash'] = data.setDefault('Etag','NULL')
    metadata['object_content_type'] = data.setDefault('Vontent-Type','NULL')
    metadata['object_content_length'] = data.setDefault('Content-Length','NULL')
    metadata['object_content_encoding'] = data.setDefault('Content-Encoding','NULL')
    metadata['object_content_disposition'] = data.setDefault('object_content_disposition','NULL')
    metadata['object_content_language'] = data.setDefault('content_language','NULL')
    metadata['object_cache_control'] = data.setDefault('cache_control','NULL')
    metadata['object_delete_at'] = data.setDefault('delete_at','NULL')
    metadata['object_manifest_type'] = data.setDefault('manifest_type','NULL')
    metadata['object_manifest'] = data.setDefault('manifest_type','0')
    metadata['object_access_control_allow_origin'] = data.setDefault('access_control_allow_origin','NULL')
    metadata['object_access_control_allow_credentials'] = data.setDefault('access_control_allow_credentials','NULL')
    metadata['object_access_control_expose_headers'] = data.setDefault('access_control_expose_headers','NULL')
    metadata['object_access_control_max_age'] = data.setDefault('access_control_max_age','NULL')
    metadata['object_access_control_allow_methods'] = data.setDefault('access_control_allow_methods','NULL')
    metadata['object_access_control_allow_headers'] = data.setDefault('access_control_allow_headers','NULL')
    metadata['object_origin'] = data.setDefault('origin','NULL')
    metadata['object_access_control_request_method'] = data.setDefault('access_control_request_method','NULL')
    metadata['object_access_control_request_headers'] = data.setDefault('access_control_request_headers','NULL')

    metajson = {}
    for meta in data:
        if(meta.startswith("X-object_meta")):
            metajson[meta] = data[meta]
    metadata['object_meta'] = json.dump(metajson)

    return metadata
