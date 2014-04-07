# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import time
import traceback
from swift import gettext_ as _

from eventlet import Timeout
import swift.common.db

from swift.metadata.backend import MetadataBroker
from swift.common.db import DatabaseAlreadyExists

from swift.common.utils import get_logger, public, \
    config_true_value, json, timing_stats, \
    split_path

from swift.common.constraints import check_utf8

from swift.common.db_replicator import ReplicatorRpc

from swift.common.swob import HTTPBadRequest, HTTPConflict, \
    HTTPInternalServerError, HTTPNoContent, \
    HTTPPreconditionFailed, HTTPMethodNotAllowed, Request, Response, \
    HTTPException

DATADIR = 'metadata'
ACCOUNT_SYS_ATTRS = [
    'account_uri',
    'account_name',
    'account_tenant_id',
    'account_first_use_time',
    'account_last_modified_time',
    'account_last_changed_time',
    'account_delete_time',
    'account_last_activity_time',
    'account_container_count',
    'account_object_count',
    'account_bytes_used']

CONTAINER_SYS_ATTRS = [
    'container_uri',
    'container_name',
    'container_account_name',
    'container_create_time',
    'container_last_modified_time',
    'container_last_changed_time',
    'container_delete_time',
    'container_last_activity_time',
    'container_read_permissions',
    'container_write_permissions',
    'container_sync_to',
    'container_sync_key',
    'container_versions_location',
    'container_object_count',
    'container_bytes_used']

OBJECT_SYS_ATTRS = [
    'object_uri',
    'object_name',
    'object_account_name',
    'object_container_name',
    'object_location',
    'object_uri_create_time',
    'object_last_modified_time',
    'object_last_changed_time',
    'object_delete_time',
    'object_last_activity_time',
    'object_etag_hash',
    'object_content_type',
    'object_content_length',
    'object_content_encoding',
    'object_content_disposition',
    'object_content_language',
    'object_cache_control',
    'object_delete_at',
    'object_manifest_type',
    'object_manifest',
    'object_access_control_allow_origin',
    'object_access_control_allow_credentials',
    'object_access_control_expose_headers',
    'object_access_control_max_age',
    'object_access_control_allow_methods',
    'object_access_control_allow_headers',
    'object_origin',
    'object_access_control_request_method',
    'object_access_control_request_headers']


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

    def check_attrs(self, attrs, acc, con, obj):
        """
        Verify that attributes are valid
        """
        for attr in attrs.split(','):
            if attr.startswith('object_meta') or \
                    attr.startswith('container_meta') or \
                    attr.startswith('account_meta'):
                pass
            elif attr not in [
                    'object_uri',
                    'object_name',
                    'object_account_name',
                    'object_container_name',
                    'object_location',
                    'object_uri_create_time',
                    'object_last_modified_time',
                    'object_last_changed_time',
                    'object_delete_time',
                    'object_last_activity_time',
                    'object_etag_hash',
                    'object_content_type',
                    'object_content_length',
                    'object_content_encoding',
                    'object_content_disposition',
                    'object_content_language',
                    'object_cache_control',
                    'object_delete_at',
                    'object_manifest_type',
                    'object_manifest',
                    'object_access_control_allow_origin',
                    'object_access_control_allow_credentials',
                    'object_access_control_expose_headers',
                    'object_access_control_max_age',
                    'object_access_control_allow_methods',
                    'object_access_control_allow_headers',
                    'object_origin',
                    'object_access_control_request_method',
                    'object_access_control_request_headers',
                    'object_meta',
                    'container_uri',
                    'container_name',
                    'container_account_name',
                    'container_create_time',
                    'container_last_modified_time',
                    'container_last_changed_time',
                    'container_delete_time',
                    'container_last_activity_time',
                    'container_read_permissions',
                    'container_write_permissions',
                    'container_sync_to',
                    'container_sync_key',
                    'container_versions_location',
                    'container_object_count',
                    'container_bytes_used',
                    'container_meta',
                    'account_uri',
                    'account_name',
                    'account_tenant_id',
                    'account_first_use_time',
                    'account_last_modified_time',
                    'account_last_changed_time',
                    'account_delete_time',
                    'account_last_activity_time',
                    'account_container_count',
                    'account_object_count',
                    'account_bytes_used',
                    'account_meta',
                    'all_attrs',
                    'all_system_attrs',
                    'all_meta_attrs',
                    'all_account_attrs'
                    'all_account_system_attrs',
                    'all_account_meta_attrs',
                    'all_container_attrs',
                    'all_container_system_attrs'
                    'all_container_meta_attrs',
                    'all_object_attrs',
                    'all_object_system_attrs'
                    'all_object_meta_attrs']:
                return False
        return True

    @public
    @timing_stats()
    def GET(self, req):
        """
        Handle HTTP GET requests
        Build SQL queries piece by piece and then execute
        Custom attributes need to be handled specially, since they exist
        in a seperate table
        """
        broker = self._get_metadata_broker()

        base_version, acc, con, obj = split_path(req.path, 1, 4, True)
        if 'attributes' in req.headers:
            attrs = req.headers['attributes']
        # if there is no attributes lists, include everything in scope
        # since no attributes passed in, there can be
        #  things from multiple levels of scope
        # Things must come from multiple tables
        # EX: Give me all metadata for `things` in
        # account scope where timestamp < ~something~
        elif obj != "" or obj is not None:
            attrs = "object_uri, container_uri, account_uri"
        elif con != "" and con is not None:
            attrs = "container_uri, account_uri"
        elif acc != "" and acc is not None:
            attrs = "account_uri"
        else:
            attrs = "object_uri, container_uri, account_uri"

        attrs, all_obj_meta, all_con_meta, all_acc_meta = \
            eval_superset(attrs.split(","))
        if self.check_attrs(attrs, acc, con, obj):
            accAttrs, conAttrs, objAttrs, superAttrs, customAttrs = \
                split_attrs_by_scope(attrs)

            accQuery = broker.get_attributes_query(acc, con, obj, accAttrs)
            conQuery = broker.get_attributes_query(acc, con, obj, conAttrs)
            objQuery = broker.get_attributes_query(acc, con, obj, objAttrs)

            if 'query' in req.headers:
                query = req.headers['query']
                accQuery = broker.get_uri_query(accQuery, query)
                conQuery = broker.get_uri_query(conQuery, query)
                objQuery = broker.get_uri_query(objQuery, query)

            ret = []
            if not accQuery.startswith("BAD"):
                ret.extend(broker.execute_query(
                    accQuery, acc, con, obj,
                    'account_uri' in attrs.split(',')))
            if not conQuery.startswith("BAD"):
                ret.extend(broker.execute_query(
                    conQuery, acc, con, obj,
                    'container_uri' in attrs.split(',')))
            if not objQuery.startswith("BAD"):
                ret.extend(broker.execute_query(
                    objQuery, acc, con, obj,
                    'object_uri' in attrs.split(',')))

            ret = broker.custom_attributes_query(customAttrs, ret)

            ret = json.dumps(ret)
            status = 200

        else:
            ret = json.dumps(attrs) + str(acc) + \
                " " + str(con) + " " + str(obj)
            status = 400
        return Response(
            request=req, body=ret + "\n", content_type="json", status=status)

    @public
    @timing_stats()
    def PUT(self, req):
        """
        Handles incoming PUT requests
        Crawlers running on the object/container/account servers
        will send over new metadata. This is where that new metadata
        is sent to the database
        """
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

        # Call broker insertion
        if 'user-agent' not in req.headers:

            return HTTPBadRequest(
                body='No user agent specified',
                request=req,
                content_type='text/plain'
            )
        md_type = req.headers['user-agent']
        md_data = json.loads(req.body)

        if not os.path.exists(broker.db_file):
            try:
                broker.initialize(time.time())
                # created = True
            except DatabaseAlreadyExists:
                # created = False
                pass
        else:
            #created = broker.is_deleted(md_type)
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
        # start_time = time.time()
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
        # trans_time = '%.4f' % (time.time() - start_time)
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


def split_attrs_by_scope(attrs):
    """
    Take the list of attributes and split them by object,container,account,
    superset, and custom.
    Reuturns a tuple of attribute strings.
    """
    acc_star = []
    con_star = []
    obj_star = []
    all_star = []
    custom_star = []
    for attr in attrs.split(','):
        if attr != "" or attr is not None:
            if attr.startswith('object_meta') or \
                    attr.startswith('container_meta') or \
                    attr.startswith('account_meta'):
                custom_star.append(attr)
            elif attr.startswith('object'):
                obj_star.append(attr)
            elif attr.startswith('container'):
                con_star.append(attr)
            elif attr.startswith('account'):
                acc_star.append(attr)
            elif attr.startswith('all'):
                all_star.append(attr)
    return (",".join(acc_star), ",".join(con_star), ",".join(obj_star),
            ",".join(all_star), ",".join(custom_star))


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI container server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return MetadataController(conf)


def eval_superset(attrs):
    """
    Take in the list of attrs and iterate through
    the list and if there is a superset attrs
    replace it with the set of attrs represented
    through by the superset attr

    If custom metadata superset attr is included,
    also return a boolean for wether or not
    to include all metadata for obj/con/acc
    since it is not possible to add these
    to the attr list. These flags will
    be used later in the query to the
    custom table

    Returns the expanded attrs as a string.
    """
    expanded = set()
    cust_obj = False
    cust_con = False
    cust_acc = False
    for attr in attrs:
        if attr == "all_attrs":
            expanded = expanded.union(set(ACCOUNT_SYS_ATTRS))
            expanded = expanded.union(set(CONTAINER_SYS_ATTRS))
            expanded = expanded.union(set(OBJECT_SYS_ATTRS))
            cust_acc = True
            cust_con = True
            cust_obj = True
        elif attr == "all_system_attrs":
            expanded = expanded.union(set(ACCOUNT_SYS_ATTRS))
            expanded = expanded.union(set(CONTAINER_SYS_ATTRS))
            expanded = expanded.union(set(OBJECT_SYS_ATTRS))
        elif attr == "all_meta_attrs":
            cust_acc = True
            cust_con = True
            cust_obj = True

        elif attr == "all_account_attrs":
            expanded = expanded.union(set(ACCOUNT_SYS_ATTRS))
            cust_acc = True
        elif attr == "all_account_system_attrs":
            expanded = expanded.union(set(ACCOUNT_SYS_ATTRS))
        elif attr == "all_account_meta_attrs":
            cust_acc = True

        elif attr == "all_container_attrs":
            expanded = expanded.union(set(CONTAINER_SYS_ATTRS))
            cust_con = True
        elif attr == "all_container_system_attrs":
            expanded = expanded.union(set(CONTAINER_SYS_ATTRS))
        elif attr == "all_container_meta_attrs":
            cust_con = True

        elif attr == "all_object_attrs":
            expanded = expanded.union(set(OBJECT_SYS_ATTRS))
            cust_obj = True
        elif attr == "all_object_system_attrs":
            expanded = expanded.union(set(OBJECT_SYS_ATTRS))
        elif attr == "all_object_meta_attrs":
            cust_obj = True
        else:
            expanded.add(attr)
    return (",".join(list(expanded)), cust_obj, cust_con, cust_acc)
