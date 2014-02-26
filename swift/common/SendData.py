from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.ring import Ring
from swift.common.http import is_success, HTTP_INTERNAL_SERVER_ERROR


class Sender():

    def __init__(self, conf):

		self.conn_timeout = float(conf.get('conn_timeout', 0.5))



    def sendData (self, metaList, data_type, server_ip, server_port, device_name):
        ip = server_ip
        port = server_port
        device = device_name
        updatedData = json.dumps(metaList)

        with ConnectionTimeout(self.conn_timeout):
            try:
                headers = {}
                conn = http_connect(
                    node['ip'], node['port'], node['device'], part,
                    'PUT', updatedData, headers=headers)

            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR account update failed with '
                    '%(ip)s:%(port)s/%(device)s (will retry later): '), node)
                return HTTP_INTERNAL_SERVER_ERROR
        with Timeout(self.node_timeout):
            try:
                resp = conn.getresponse()
                resp.read()
                return resp.status
            except (Exception, Timeout):
                if self.logger.getEffectiveLevel() <= logging.DEBUG:
                    self.logger.exception(
                        _('Exception with %(ip)s:%(port)s/%(device)s'), node)
                return HTTP_INTERNAL_SERVER_ERROR
            finally:
                conn.close()
