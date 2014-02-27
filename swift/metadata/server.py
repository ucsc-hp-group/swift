#
# Metadata server
#

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

from swift.common.utils import get_logger, hash_path, public, \
    normalize_timestamp, storage_directory, validate_sync_to, \
    config_true_value, json, timing_stats, replication, \
    override_bytes_from_content_type

from swift.common.constraints import ACCOUNT_LISTING_LIMIT, \
CONTAINER_LISTING_LIMIT, check_mount, check_float, check_utf8

from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.db_replicator import ReplicatorRpc
from swift.common.http import HTTP_NOT_FOUND, is_success

from swift.common.swob import HTTPAccepted, HTTPBadRequest, HTTPConflict, \
    HTTPCreated, HTTPInternalServerError, HTTPNoContent, HTTPNotFound, \
    HTTPPreconditionFailed, HTTPMethodNotAllowed, Request, Response, \
    HTTPInsufficientStorage, HTTPException, HeaderKeyDict

from swift.metadata.utils import metadata_listing_response, \
    metadata_deleted_response

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

        self.auto_create_account_prefix = conf.get('auto_create_account_prefix') 
            or '.'

        if config_true_value(conf.get('allow_versions', 'f')):
            self.save_headers.append('x-versions-location')

        swift.common.db.DB_PREALLOCATION = config_true_value(
            conf.get('db_preallocation', 'f'))

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

    @public
    @timing_stats
    def GET(self, req):
        # Handle HTTP GET requests
        drive, partition, account = split_and_validate_path(req, 3)
        prefix = get_param(req, 'prefix')
        delimiter = get_param(req, 'delimiter')

        if delimiter and (len(delimiter) > 1 or ord(delimiter) > 254):
            return HTTPPreconditionFailed(body="Bad Delimiter")

        listing_limit = min(ACCOUNT_LISTING_LIMIT, CONTAINER_LISTING_LIMIT)
        given_limit = get_param(req, 'limit')

        if given_limit and given_limit.isdigit():
            limit = int(given_limit)
            if limit > listing_limit:
                return HTTPPreconditionFailed(
                    request=req, body="Max limit is %d" % listing_limit)

        marker = get_param(req, 'marker', '')
        end_marker = get_param(req, 'end_marker')
        out_content_type = get_listing_content_type(req)

        # TODO: mount check

        broker = self._get_metadata_broker(drive, partition, account, 
            pending_timeout=0.1, stale_reads_ok=True)

        return metadata_listing_response(account, req, out_content_type, broker, 
            limit, marker, end_marker, prefix, delimiter)

    @public
    @timing_stats
    def POST(self, req):
        drive, partition, account = split_and_validate_path(req, 3)

        if 'x-timestamp' not in req.headers \
                or not check_float(req.headers['x-timestamp']):
            return HTTPBadRequest(
                body='Missing or bad timestamp',
                request=req,
                content_type='text/plain'
            )

        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive,request=req)

        broker = self._get_metadata_broker(drive, partition, account)

        if broker.is_deleted():
            return metadata_deleted_response(broker, req, HTTPNotFound)

        timestamp = normalize_timestamp(req.headers['x-timestamp'])
        metadata = {}

        # Call broker insertion
        if 'user-agent' not in req.headers:
            return HTTPBadRequest(
                body='No user agent specified',
                request=req,
                content_type='text/plain'
            )
        md_type = req.headers['user-agent']
        md_data = json.loads(req.body)
        
        for item in md_data:
            # check the user agent type
            if md_type == 'account_crawler':
                # insert accounts
                broker.insert_account_md(item)
            elif md_type == 'container_crawler':
                # Insert containers
                broker.insert_container_md(item)
            elif md_type == 'object_crawler':
                # Insert object
                broker.insert_object_md(item)
            else
                # raise exception
                return HTTPBadRequest(
                    body='Invalid user agent',
                    request=req,
                    content_type='text/plain'
                )

        return HTTPNoContent(request=req)



def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI container server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return MetadataController(conf)