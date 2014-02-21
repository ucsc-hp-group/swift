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
import signal
import sys
import time
from swift import gettext_ as _
from random import random

from eventlet import patcher, Timeout

from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.ring import Ring
from swift.common.utils import get_logger, renamer, write_pickle, \
    dump_recon_cache, config_true_value, ismount, json
from swift.common.daemon import Daemon
from swift.obj.diskfile import *
from swift.common.http import is_success, HTTP_NOT_FOUND, \
    HTTP_INTERNAL_SERVER_ERROR


class ObjectCrawler(Daemon):
    """Update object information in container listings."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='object-updater')
        self.devices = conf.get('devices', '/srv/node')
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
                    f.write("Timeout ERROR!!!!\n")
            time.sleep(self.interval)

    def run_once(self, *args, **kwargs):
        """Run the updater once."""
        self.object_sweep()

    def object_sweep(self):
        """
        Scan through all objects and return meta data dict

        :param device: path to device
        """

         
        all_locs = self.diskfile_mgr.object_audit_location_generator()
        metaList = []
        for location in all_locs:
            metaDict = self.collect_object(location)
            if metaDict != {}:
                metaList.append(metaDict)
        #sending.send(metaList)
        with open("/opt/stack/data/swift/logs/obj-crawler.log", "a+") as f:
            f.write(json.dumps(metaList))
        

    def collect_object(self, location):
        """
        Process the object metadata

        :param update_path: path to pickled object update file
        :param device: path to device
        """
        metadata = {}
        try:

            df = self.diskfile_mgr.get_diskfile_from_audit_location(location)
            metadata = df.read_metadata()
        except DiskFileNotExist:
            with open("/opt/stack/data/swift/logs/obj-crawler.log", "a+") as f:
                f.write("DISKFILE DOES NOT EXIST\n")
        return metadata