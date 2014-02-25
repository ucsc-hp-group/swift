
import os, time, traceback
from datetime import datetime
from swift import gettext_ as _
from xml.etree.cElementTree import Element, SubElement, tostring

from eventlet import Timeout
import swift.common.db

from swift.metadata.backend import MetadataBroker
from swift.common.db import DatabaseAlreadyExists
from swift.common.request_helpers import get_param, \
    get_listing_content_type, split_and_validate_path, is_sys_or_user_meta

from swift.common.utils import get_logger, hash_path, public, normalize_timestamp, storage_directory, \
    validate_sync_to, config_true_value, json, timing_stats, replication, override_bytes_from_content_type

from swift.common.constraints import CONTAINER_LISTING_LIMIT, check_mount, check_float, check_utf8
from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.db_replicator import ReplicatorRpc
from swift.common.http import HTTP_NOT_FOUND, is_success

from swift.common.swob import HTTPAccepted, HTTPBadRequest, HTTPConflict, HTTPCreated, HTTPInternalServerError,\
    HTTPNoContent, HTTPNotFound,HTTPPreconditionFailed, HTTPMethodNotAllowed, Request, Response, \
    HTTPInsufficientStorage, HTTPException, HeaderKeyDict

DATADIR = 'metadata'

class MetadataController(object):
    # WSGI Controller for metadata server
    save_headers = [
        'x-metadata-read',
        'x-metadata-write',
        'x-metadata-sync-key',
        'x-metadata-sync-to'
    ]

    def __init__(self, conf, logger=None):
        self.logger = logger or get_logger(conf, log_route='metadata-server')
        self.root = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.node_timeout = int(conf.get('node_timeout', 3))
        self.conn_timeout = float(conf.get('node_timeout', 3))
        replication_server = conf.get('replication_server', None)
        if replication_server is not None:
            replication_server = config_true_value(replication_server)
        self.replication_server = replication_server
        self.allowed_sync_hosts = [
            h.strip()
            for h in conf.get('allowed_sync_hosts', '127.0.0.1').split(',')
            if h.strip()
        ]
        self.replicator_rpc = ReplicatorRpc(
            self.root,
            DATADIR,
            MetadataBroker,
            self.mount_check,
            logger=self.logger
        )
        self.auto_create_account_prefix = conf.get('auto_create_account_prefix') or '.'
        if config_true_value(conf.get('allow_versions', 'f')):
            self.save_headers.append('x-versions-location')
        swift.common.db.DB_PREALLOCATION = config_true_value(conf.get('db_preallocation', 'f'))

    def _get_metadata_broker(self, drive, part, account, container, **kwargs):
        """
        Get a DB broker for the metadata
        """
        hash = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, hash)
        db_path = os.path.join(self.root, drive, drive)
        kwargs.setdefault('account', account)
        kwargs.setdefault('container', container)
        kwargs.setdefault('logger', self.logger)
        return MetadataBroker(**kwargs)

    '''
    # Implementation WIP
    @public
    @timing_stats
    def DELETE(self, req):
        # Handle HTTP DELETE requests

    @public
    @timing_stats
    def PUT(self, req):
        # Handle HTTP PUT requests

    @public
    @timing_stats
    def HEAD(self, req):
        # Handle HTTP HEAD requests

    @public
    @timing_stats
    def GET(self, req):
        # Handle HTTP GET requests

    @public
    @timing_stats
    def POST(self, req):
        # Handle HTTP POST requests

    '''

def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI container server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return MetadataController(conf)