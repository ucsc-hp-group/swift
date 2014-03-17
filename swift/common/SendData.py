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

from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.ring import Ring
from swift.common.http import is_success, HTTP_INTERNAL_SERVER_ERROR
from swift.common.utils import json
from eventlet import Timeout
from eventlet.green.httplib import HTTPConnection, HTTPResponse
class Sender():

    def __init__(self, conf):

        self.conn_timeout = float(conf.get('conn_timeout', 3))

    def sendData (self, metaList, data_type, server_ip, server_port):
        ip = server_ip
        port = server_port
        updatedData = json.dumps(metaList)
        headers = {'user-agent': data_type}
        with ConnectionTimeout(self.conn_timeout):
            try:
                conn = HTTPConnection('%s:%s' % (ip, port))
                conn.request('PUT', '/', headers=headers, body=updatedData)
                resp = conn.getresponse()
                return resp
            except (Exception, Timeout):
                return HTTP_INTERNAL_SERVER_ERROR

        # with Timeout(self.conn_timeout):
        #     try:
        #         resp = conn.getresponse()
        #         resp.read()
        #         return resp.status
        #     except (Exception, Timeout):
        #         return HTTP_INTERNAL_SERVER_ERROR
        #     finally:
        #         conn.close()
