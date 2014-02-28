
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
