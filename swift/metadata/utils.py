
import time
from xml.sax import saxutils

from swift.common.swob import HTTPOk, HTTPNoContent
from swift.common.utils import json, normalize_timestamp
from swift.common.db import DatabaseConnectionError

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
def metadata_listing_response(account, req, response_content_type, broker=None, 
    limit='', marker='', end_marker='', prefix='', delimiter=''):
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

def metadata_deleted_response(self, broker, req, resp, body=''):
    headers = {}
    try:
        if broker.is_status_deleted():
            # Account does exist and is marked for deletion
            headers = {'X-Account-Status': 'Deleted'}
    except DatabaseConnectionError:
        # Account does not exist!
        pass
    return resp(request=req, headers=headers, charset='utf-8', body=body)

"""
'schema' is a dict that looks like this:
    {
        'table': <table name>,
        'columns': [..., {
            'name': <column name>,
            'type': <TEXT/INTEGER>,
            'opts': [...,<DEFAULT/UNIQUE/...>] (?)
        }]
    }
"""

# build a query that creates a table from a given schema
def build_create_table_sql(schema):
    query = '''
        CREATE TABLE %s(
            ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
            %s
        );
    '''

    ''.join([
            '%s %s,' % (col['name'] , col['type'])
        if i < len(schema['columns']) - 1
        else
            '%s %s' % (col['name'] , col ['type'])
        for i, col in enumerate(schema['columns']) 
    ])

    return query % (schema['table'], qcols)

# build a query that inserts an entry, or updates an extant
# entry in place
def build_insert_sql(schema):
    # schema should be a set of dicts
    query = 'INSERT INTO %s(%s) VALUES(%s) ON DUPLICATE KEY UPDATE %s;'

    numcols = len(schema['columns']) - 1
    qcols = ''.join([
            '%s,' % col['name']
        if i < numcols
        else
            '%s' % col['name']
        for i, col in enumerate(schema['columns'])
    ])
    qtypes = ''.join([
                u'\u0025s, '
            if col['type'] == 'TEXT' or col['type'] == "TEXT DEFAULT '0'"
            else
                u'\u0025d, '
        if i < numcols
        else
                u'\u0025s'
            if col['type'] == 'TEXT'
            else
                u'\u0025d'
        for i, col in enumerate(schema['columns'])
    ])
    qupdates = ''.join([
            '%s = VALUES(%s), ' % (col['name'], col['name'])
        if i < numcols
        else
            '%s = VALUES(%s)' % (col['name'], col['name']) 
        for i, col in enumerate(schema['columns'])
    ])
    return query % (schema['table'], qcols, qtypes, qupdates)

# Utilities to provide the valid columns of each table
def get_account_md_schema():
    return {
        'account_uri': 'TEXT PRIMARY KEY',
        'account_name': 'TEXT',
        'account_tenant_id': 'TEXT',
        'account_first_use_time': 'TEXT',
        'account_last_modified_time': 'TEXT',
        'account_last_changed_time': 'TEXT',
        'account_delete_time: TEXT',
        'account_last_activity_time': 'TEXT',
        'account_container_count': 'INTEGER',
        'account_object_count': 'INTEGER',
        'account_bytes_used': 'INTEGER',
        'account_meta': 'TEXT'
    }

def get_container_md_schema():
    return {
        'container_uri': 'TEXT PRIMARY KEY',
        'container_name': 'TEXT',
        'container_account_name': 'TEXT',
        'container_create_time': 'TEXT',
        'container_last_modified_time': 'TEXT',
        'container_last_changed_time': 'TEXT',
        'container_delete_time': 'TEXT',
        'container_last_activity_time': 'TEXT',
        'container_read_permissions': 'TEXT',
        'container_write_permissions': 'TEXT',
        'container_sync_to': 'TEXT',
        'container_sync_key': 'TEXT',
        'container_versions_location': 'TEXT',
        'container_object_count': 'INTEGER',
        'container_bytes_used': 'INTEGER',
        'container_meta': 'TEXT'
    }

def get_object_md_schema():
    return {
        'object_uri' : 'TEXT PRIMARY KEY',
        'object_name': 'TEXT',
        'object_account_name': 'TEXT',
        'object_container_name': 'TEXT',
        'object_location': 'TEXT',
        'object_uri_create_time': 'TEXT',
        'object_last_modified_time': 'TEXT',
        'object_last_changed_time': 'TEXT',
        'object_delete_time': 'TEXT',
        'object_last_activity_time': 'TEXT',
        'object_etag_hash': 'TEXT',
        'object_content_type': 'TEXT',
        'object_content_length': 'INTEGER',
        'object_content_encoding': 'TEXT',
        'object_content_disposition': 'TEXT',
        'object_content_language': 'TEXT',
        'object_cache_control': 'TEXT',
        'object_delete_at': 'TEXT',
        'object_manifest_type': 'INTEGER',
        'object_manifest': 'TEXT',
        'object_access_control_allow_origin': 'TEXT',
        'object_access_control_allow_credentials': 'TEXT',
        'object_access_control_expose_headers': 'TEXT',
        'object_access_control_max_age': 'TEXT',
        'object_access_control_allow_methods': 'TEXT',
        'object_access_control_allow_headers': 'TEXT',
        'object_origin': 'TEXT',
        'object_access_control_request_method': 'TEXT',
        'object_access_control_request_headers': 'TEXT',
        'object_meta': 'TEXT'
    }

# check for a column name inside some schema dict
def cross_reference_md_schema(query, table):
    if table == 'object_metadata' and query in get_object_md_schema():
        return True
    elif table == 'container_metadata' and query in get_container_md_schema():
        return True
    elif table == 'account_metadata' and query in get_account_md_schema():
        return True
    else:
        return False