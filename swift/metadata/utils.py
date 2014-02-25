
import time
from xml.sax import saxutils

from swift.common.swob import HTTPOk, HTTPNoContent
from swift.common.utils import json, normalize_timestamp

# Fake metadata broker
class FakeMetadataBroker(object):

    def get_info(self):
        now = normalize_timestamp(time.time())

        return {
            'object_count': 0,
            'container_count': 0,
            'bytes_used': 0,
            'created_at': now,
            'put_timestamp': now
        }

    def list_meta_iter(self, *_, **__):
        return []

# Construct a listing response from metadata DB
def metadata_listing_response(account, req, response_content_type, broker=None, limit='', marker='', end_marker='',
                              prefix='', delimiter=''):
    if broker is None:
        broker = FakeMetadataBroker()
    info = broker.get_info()

    resp_headers = {
        'X-Metadata-Container-Count': info['container_count'],
        'X-Metadata-Object-Count': info['object_count'],
        'X-Metadata-Bytes-Used': info['bytes_used'],
        'X-Timestamp': info['created_at'],
        'X-PUT-Timestamp': info['put_timestamp']
    }

    meta_list = broker.list_meta_iter(limit, marker, end_marker, prefix, delimiter)

    # List comprehensions for fun and profit
    if response_content_type == 'application/json':
        meta_list = json.dumps([
            {
                'subdir': name
            }
            if is_subdir
            else
            {
                'subdir': name,
                'count': object_count,
                'bytes': bytes_used
            }
            for (name, object_count, bytes_used, is_subdir) in meta_list
        ])
    elif response_content_type.endswith('/xml'):
        meta_list = '\n'.join([
                '<?xml version="1.0" encoding="UTF-8"?>',
                '<account name=%s>' % saxutils.quoteattr(account)
            ] + [
                '<subdir name=%s />' % saxutils.quoteattr(name)
                if is_subdir
                else
                '<container><name>%s</name><count>%s</count><bytes>%s</bytes></container>' % \
                    (saxutils.escape(name), object_count, bytes_used)
                for (name, object_count, bytes_used, is_subdir) in meta_list
            ] + ['</account>']
        )
    else:
        if not meta_list:
            resp = HTTPNoContent(request=req, headers=resp_headers)
            resp.content_type = response_content_type
            resp.charset = 'utf-8'
            return resp
        meta_list = '\n'.join(r[0] for r in meta_list + '\n')

    ret = HTTPOk(body=meta_list, request=req, headers=resp_headers)
    ret.content_type = response_content_type
    ret.charset = 'utf-8'
    return ret