
import os, time, errno, sqlite3
from uuid import uuid4
from swift.common.utils import normalize_timestamp, lock_parent_directory
from swift.common.db import DatabaseBroker, DatabaseConnectionError, \
    PENDING_CAP, PICKLE_PROTOCOL, utf8encode

import cPickle as pickle

from swift.metadata.utils import build_insert_sql

# Interface with metadata database
class MetadataBroker(DatabaseBroker):

    type = 'metadata'
    db_contains_type = 'object'
    db_reclaim_timestamp = 'created_at'

    # Initialize DB
    def _initialize(self, conn, timestamp):
        # Create metadata tables
        self.create_account_md_table(conn)
        self.create_container_md_table(conn)
        self.create_object_md_table(conn)

    def create_account_md_table(self, conn):
        conn.executescript("""
            CREATE TABLE account_metadata (
                account_uri TEXT PRIMARY KEY,
                account_name TEXT,
                account_tenant_id TEXT,
                account_first_use_time TEXT DEFAULT '0',
                account_last_modified_time TEXT DEFAULT '0',
                account_last_changed_time TEXT DEFAULT '0',
                account_delete_time TEXT DEFAULT '0',
                account_last_activity_time TEXT DEFAULT '0',
                account_container_count INTEGER,
                account_object_count INTEGER,
                account_bytes_used INTEGER,
                account_meta TEXT
            );
        """)

    def create_container_md_table(self, conn):
        conn.executescript("""
            CREATE TABLE container_metadata (
                container_uri TEXT PRIMARY KEY,
                container_name TEXT,
                container_account_name TEXT,
                container_create_time TEXT DEFAULT '0',
                container_last_modified_time TEXT DEFAULT '0',
                container_last_changed_time TEXT DEFAULT '0',
                container_delete_time TEXT DEFAULT '0',
                container_last_activity_time TEXT DEFAULT '0',
                container_read_permissions TEXT,
                container_write_permissions TEXT,
                container_sync_to TEXT,
                container_sync_key TEXT,
                container_versions_location TEXT,
                container_object_count INTEGER,
                container_bytes_used INTEGER,
                container_meta TEXT
            );
        """)

    def create_object_md_table(self, conn):
        conn.executescript("""
            CREATE TABLE object_metadata (
                object_uri TEXT PRIMARY KEY,
                object_name TEXT,
                object_account_name TEXT,
                object_container_name TEXT,
                object_location TEXT,
                object_uri_create_time TEXT DEFAULT '0',
                object_last_modified_time TEXT DEFAULT '0',
                object_last_changed_time TEXT DEFAULT '0',
                object_delete_time TEXT DEFAULT '0',
                object_last_activity_time TEXT DEFAULT '0',
                object_etag_hash TEXT,
                object_content_type TEXT,
                object_content_length INTEGER,
                object_content_encoding TEXT,
                object_content_disposition TEXT,
                object_content_language TEXT,
                object_cache_control TEXT,
                object_delete_at TEXT DEFAULT '0',
                object_manifest_type INTEGER,
                object_manifest TEXT,
                object_access_control_allow_origin TEXT,
                object_access_control_allow_credentials TEXT,
                object_access_control_expose_headers TEXT,
                object_access_control_max_age TEXT,
                object_access_control_allow_methods TEXT,
                object_access_control_allow_headers TEXT,
                object_origin TEXT,
                object_access_control_request_method TEXT,
                object_access_control_request_headers TEXT,
                object_meta TEXT
            );
        """)

    # Data insertion methods
    def insert_account_md(self, data):
        with self.get() as conn:
            query = '''
                INSERT OR REPLACE INTO account_metadata (
                    account_uri,
                    account_name,
                    account_tenant_id,
                    account_first_use_time,
                    account_last_modified_time,
                    account_last_changed_time,
                    account_delete_time,
                    account_last_activity_time,
                    account_container_count,
                    account_object_count,
                    account_bytes_used,
                    account_meta
                )
                VALUES ("%s","%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,"%s")
                ;
            '''
            # Build and execute query for each requested insertion
            conn.commit()
            for item in data:
                formatted_query = query % (
                    item['account_uri'],
                    item['account_name'],
                    item['account_tenant_id'],
                    item['account_first_use_time'],
                    item['account_last_modified_time'],
                    item['account_last_changed_time'],
                    item['account_delete_time'],
                    item['account_last_activity_time'],
                    item['account_container_count'],
                    item['account_object_count'],
                    item['account_bytes_used'],
                    item['account_meta']
                )
                conn.execute(formatted_query)
            conn.commit()

    def insert_container_md(self, data):
        with self.get() as conn:
            query = '''
                INSERT OR REPLACE INTO container_metadata (
                    container_uri,
                    container_name,
                    container_account_name,
                    container_create_time,
                    container_last_modified_time,
                    container_last_changed_time,
                    container_delete_time,
                    container_last_activity_time,
                    container_read_permissions,
                    container_write_permissions,
                    container_sync_to,
                    container_sync_key,
                    container_versions_location,
                    container_object_count,
                    container_bytes_used,
                    container_meta
                )
                VALUES (
                    "%s", "%s", "%s", %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, "%s" 
                )
                ;
            '''
            conn.commit()
            for item in data:
                formatted_query = query % (
                    item['container_uri'],
                    item['container_name'],
                    item['container_account_name'],
                    item['container_create_time'],
                    item['container_last_modified_time'],
                    item['container_last_changed_time'],
                    item['container_delete_time'],
                    item['container_last_activity_time'],
                    item['container_read_permissions'],
                    item['container_write_permissions'],
                    item['container_sync_to'],
                    item['container_sync_key'],
                    item['container_versions_location'],
                    item['container_object_count'],
                    item['container_bytes_used'],
                    item['container_meta']
                )
                with open("/opt/stack/data/swift/logs/delete.log", "w") as f:
                    f.write(formatted_query)
                conn.execute(formatted_query)
            conn.commit()

    def insert_object_md(self, data):
        with self.get() as conn:
            query = '''
                INSERT OR REPLACE INTO object_metadata (
                    object_uri,
                    object_name,
                    object_account_name,
                    object_container_name,
                    object_location,
                    object_uri_create_time,
                    object_last_modified_time,
                    object_last_changed_time,
                    object_delete_time,
                    object_last_activity_time,
                    object_etag_hash,
                    object_content_type,
                    object_content_length,
                    object_content_encoding,
                    object_content_disposition,
                    object_content_language,
                    object_cache_control,
                    object_delete_at,
                    object_manifest_type,
                    object_manifest,
                    object_access_control_allow_origin,
                    object_access_control_allow_credentials,
                    object_access_control_expose_headers,
                    object_access_control_max_age,
                    object_access_control_allow_methods,
                    object_access_control_allow_headers,
                    object_origin,
                    object_access_control_request_method,
                    object_access_control_request_headers,
                    object_meta
                ) VALUES (
                    "%s", "%s", "%s", "%s", %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, "%s" 
                )
                ;
            '''
            conn.commit()
            for item in data:
                formatted_query = query % (
                    item['object_uri'],
                    item['object_name'],
                    item['object_account_name'],
                    item['object_container_name'],
                    item['object_location'],
                    item['object_uri_create_time'],
                    item['object_last_modified_time'],
                    item['object_last_changed_time'],
                    item['object_delete_time'],
                    item['object_last_activity_time'],
                    item['object_etag_hash'],
                    item['object_content_type'],
                    item['object_content_length'],
                    item['object_content_encoding'],
                    item['object_content_disposition'],
                    item['object_content_language'],
                    item['object_cache_control'],
                    item['object_delete_at'],
                    item['object_manifest_type'],
                    item['object_manifest'],
                    item['object_access_control_allow_origin'],
                    item['object_access_control_allow_credentials'],
                    item['object_access_control_expose_headers'],
                    item['object_access_control_max_age'],
                    item['object_access_control_allow_methods'],
                    item['object_access_control_allow_headers'],
                    item['object_origin'],
                    item['object_access_control_request_method'],
                    item['object_access_control_request_headers'],
                    item['object_meta']
                )
                conn.execute(formatted_query)
            conn.commit()

    def is_deleted(self, mdtable, timestamp=None):
        '''
        Determine whether a DB is considered deleted
        :param mdtable: a string representing the relevant object type (account, 
            container, object)
        :returns: True if the DB is considered deleted, False otherwise
        '''
        if self.db_file != ':memory:' and not os.path.exists(self.db_file):
            return True
        self._commit_puts_stale_ok()
        return False
        # with self.get() as conn:
        #     query = '''
        #         SELECT put_timestamp, delete_timestamp, object_count
        #         FROM %s_metadata
        #     ''' % (mdtable.split('_')[0])
        #     row = conn.execute(query).fetchone()
        #     if timestamp and row['delete_timestamp'] > timestamp:
        #         return False
        #     return (row['object_count'] in (None, '', 0, '0')) and \
        #         (float(row['delete_timestamp']) > float(row['put_timestamp']))
