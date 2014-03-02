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

import os, json
from swift.common.swob import Request, Response
from eventlet.green.httplib import HTTPConnection
from urllib import urlencode

class MetaDataMiddleware(object):
    """
    Middleware for metadata queries. See OSMS API

    """

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.mds_ip = conf.get('md-server-ip', '127.0.0.1')
        self.mds_port = conf.get('md-server-port', '6090')
        self.version = 'v1'

    def GET(self, req):
        """Handle the query request."""
        conn = HTTPConnection('%s:%s' % (self.mds_ip, self.mds_port))
        headers = req.params
        conn.request('GET', req.path, headers=headers)
        resp = conn.getresponse()
        return Response(request=req, body=resp.read(), content_type="json")

    def BAD(self, req):
        """Returns a 400 for bad request"""
        return Response(request=req, status=400, body="Metadata version bad\n", content_type="text/plain")

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            if 'metadata' in req.params: 
                if req.params['metadata'] == 'v1':
                    handler = self.GET
                    return handler(req)(env, start_response)
                else:
                    handler = self.BAD
                    return handler(req)(env, start_response)
        except UnicodeError:
            pass
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def metadata_filter(app):
        return MetaDataMiddleware(app, conf)
    return metadata_filter
