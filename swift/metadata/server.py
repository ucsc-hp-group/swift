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
    HTTPInsufficientStorage, HTTPException, HeaderKeyDict, HTTPOk

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
        self.location = conf.get('location', '/srv/node/sdb1/metadata/')
        self.db_file = os.path.join(self.location, 'meta.db')
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

        # self.auto_create_account_prefix = conf.get('auto_create_account_prefix') 
        #     or '.'

        if config_true_value(conf.get('allow_versions', 'f')):
            self.save_headers.append('x-versions-location')

        swift.common.db.DB_PREALLOCATION = config_true_value(
            conf.get('db_preallocation', 'f'))

    def _get_metadata_broker(self, **kwargs):
        """
        Get a DB broker for the metadata
        """
        # hash = hash_path(account, container)
        # db_dir = storage_directory(DATADIR, part, hash)
        # db_path = os.path.join(self.root, drive, drive)
        # kwargs.setdefault('account', account)
        # kwargs.setdefault('container', container)
        # kwargs.setdefault('logger', self.logger)
        kwargs.setdefault('db_file', self.db_file)
        return MetadataBroker(**kwargs)

    @public
    @timing_stats()
    def GET(self, req):
        # Handle HTTP GET requests
        broker = self._get_metadata_broker()
        listOfMD = broker.getAll()
        return Response(request=req, body=listOfMD, content_type="text/plain")

    @public
    @timing_stats()
    def PUT(self, req):
        #drive, partition, account = split_and_validate_path(req, 3)

        # if 'x-timestamp' not in req.headers \
        #         or not check_float(req.headers['x-timestamp']):
        #     return HTTPBadRequest(
        #         body='Missing or bad timestamp',
        #         request=req,
        #         content_type='text/plain'
        #     )

        # if self.mount_check and not check_mount(self.root, drive):
        #     return HTTPInsufficientStorage(drive=drive,request=req)

        broker = self._get_metadata_broker()

        # if broker.is_deleted():
        #     return metadata_deleted_response(broker, req, HTTPNotFound)

        # timestamp = normalize_timestamp(req.headers['x-timestamp'])
        metadata = {}

        # Call broker insertion
        if 'user-agent' not in req.headers:

            return HTTPBadRequest(
                body='No user agent specified',
                request=req,
                content_type='text/plain'
            )
        with open("/opt/stack/data/swift/logs/metaserver.log", "a+") as f:
            f.write(req.headers['user_agent'] + req.body + "\n")
        md_type = req.headers['user-agent']
        md_data = json.loads(req.body)
        
        if not os.path.exists(broker.db_file):
            try:
                broker.initialize(time.time())
                created = True
            except DatabaseAlreadyExists:
                created = False
        else:
            created = broker.is_deleted(md_type)
            # broker.update_put_timestamp(time.time())
            if broker.is_deleted(md_type):
                return HTTPConflict(request=req)

        # check the user agent type
        if md_type == 'account_crawler':
            # insert accounts
            broker.insert_account_md(md_data)
        elif md_type == 'container_crawler':
            # Insert containers
            broker.insert_container_md(md_data)
        elif md_type == 'object_crawler':
            # Insert object
            broker.insert_object_md(md_data)
        else:
            # raise exception
            return HTTPBadRequest(
                body='Invalid user agent',
                request=req,
                content_type='text/plain'
            )
        return HTTPNoContent(request=req)

    def __call__(self, env, start_response):
        start_time = time.time()
        req = Request(env)
        self.logger.txn_id = req.headers.get('x-trans-id', None)
        if not check_utf8(req.path_info):
            res = HTTPPreconditionFailed(body='Invalid UTF8 or contains NULL')
        else:
            try:
                # disallow methods which have not been marked 'public'
                try:
                    method = getattr(self, req.method)
                    getattr(method, 'publicly_accessible')
                    replication_method = getattr(method, 'replication', False)
                    if (self.replication_server is not None and
                            self.replication_server != replication_method):
                        raise AttributeError('Not allowed method.')
                except AttributeError:
                    res = HTTPMethodNotAllowed()
                else:
                    res = method(req)
            except HTTPException as error_response:
                res = error_response
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR __call__ error with %(method)s %(path)s '),
                    {'method': req.method, 'path': req.path})
                res = HTTPInternalServerError(body=traceback.format_exc())
        trans_time = '%.4f' % (time.time() - start_time)
        # if self.log_requests:
        #     log_message = '%s - - [%s] "%s %s" %s %s "%s" "%s" "%s" %s' % (
        #         req.remote_addr,
        #         time.strftime('%d/%b/%Y:%H:%M:%S +0000',
        #                       time.gmtime()),
        #         req.method, req.path,
        #         res.status.split()[0], res.content_length or '-',
        #         req.headers.get('x-trans-id', '-'),
        #         req.referer or '-', req.user_agent or '-',
        #         trans_time)
        #     if req.method.upper() == 'REPLICATE':
        #         self.logger.debug(log_message)
        #     else:
        #         self.logger.info(log_message)
        return res(env, start_response)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI container server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return MetadataController(conf)