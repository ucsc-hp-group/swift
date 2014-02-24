'''
server.py 
Metadata Database Broker
'''

from swift.common.db import DatabaseBroker

# Interface with metadata database
class MetaDatabase(DatabaseBroker):

    # Initialize DB
    def _initialize(self, conn, put_timestamp):
        # Create metadata tables
        self.create_account_md_table(conn)
        self.create_container_md_table(conn)
        self.create_object_md_table(conn)

    def create_account_md_table(self, conn):
        conn.executescript("""
            CREATE TABLE account_metadata (
                ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
                account_uri TEXT,
                account_name TEXT UNIQUE,
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
                ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
                container_uri TEXT,
                container_name TEXT UNIQUE,
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

            CREATE UNIQUE INDEX uid_container ON container_metadata(ROWID);
        """)

    def create_object_md_table(self, conn):

        conn.executescript("""
            CREATE TABLE object_metadata (
                ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
                object_uri TEXT,
                object_name TEXT UNIQUE,
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

    # Insert metadata into the DB
    def insert_account_md(self, conn, data):      
        # Query template
        query = """
            INSERT INTO account_metadata (
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
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,%s)
            ON DUPLICATE KEY UPDATE
                account_uri                = VALUES(account_uri),
                account_name               = VALUES(account_name),
                account_tenant_id          = VALUES(account_tenant_id),
                account_first_use_time     = VALUES(account_first_use_time),
                account_last_modified_time = VALUES(account_last_modified_time),
                account_last_changed_time  = VALUES(account_last_changed_time),
                account_delete_time        = VALUES(account_delete_time),
                account_last_activity_time = VALUES(account_last_activity_time),
                account_container_count    = VALUES(account_container_count),
                account_object_count       = VALUES(account_object_count),
                account_bytes_used         = VALUES(account_bytes_used),
                account_meta               = VALUES(account_meta)
            ;
        """
        # Build and execute query for each requested insertion
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
                item['account_bytes_used'],
                item['account_meta']            
            )
            conn.executescript(formatted_query)
