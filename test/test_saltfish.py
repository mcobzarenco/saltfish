#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import sys
import argparse
import multiprocessing
import logging
import unicodedata
from os.path import join
from copy import deepcopy
from random import randint

import google.protobuf
import rpcz
from rpcz.rpc import RpcDeadlineExceeded
from rpcz import compiler
# import mysql.connector
import pymysql
import riak
import uuid


# TODO: CLEAN UP IMPORTS


PROTO_ROOT = join('..', 'src', 'proto')
DEFAULT_CONNECT = 'tcp://localhost:5555'
DEFAULT_RIAK_HOST = 'localhost'
DEFAULT_RIAK_PORT = 10017

SOURCES_META_BUCKET = '/ml/sources/schemas/'
SOURCES_DATA_BUCKET_ROOT = '/ml/sources/data/'

TEST_PASSED = '*** OK ***'
FAILED_PREFIX = 'TEST FAILED | '

DEFAULT_DEADLINE = 2


log = multiprocessing.log_to_stderr()
log.setLevel(logging.INFO)

compiler.generate_proto(join(PROTO_ROOT, 'config.proto'), '.')
compiler.generate_proto(join(PROTO_ROOT, 'source.proto'), '.')
compiler.generate_proto(join(PROTO_ROOT, 'service.proto'), '.')
compiler.generate_proto(join(PROTO_ROOT, 'service.proto'), '.',
                        with_plugin='python_rpcz', suffix='_rpcz.py')
import config_pb2
import source_pb2
import service_pb2
import service_rpcz


def make_create_source_req(source_id=None, name=None, features=None):
    features = features or []
    request = service_pb2.CreateSourceRequest()
    if source_id:
        request.source.source_id = source_id
    if name:
        request.source.name = name
    for f in features:
        new_feat = request.source.schema.features.add()
        new_feat.name = f['name']
        new_feat.feature_type = f['type']
    return request


def filter_by_type(seq, typ):
    return filter(lambda val: isinstance(val, typ), seq)


def make_put_records_req(source_id, record_ids=None, records=None):
    records = records or []
    request = service_pb2.PutRecordsRequest()
    request.source_id = str(source_id)
    if record_ids:
        request.record_ids.extend(record_ids)
    for record in records:
        new_record = request.records.add()
        new_record.reals.extend(filter_by_type(record, float))
        new_record.cats.extend(filter_by_type(record, str))
    return request


class BinaryString(str):
    def encode(self, ignored):
        return self


class SaltfishTests(unittest.TestCase):
    _config = config_pb2.Saltfish()

    @classmethod
    def configure(cls, config):
        cls._config = config

    @classmethod
    def setUpClass(cls):
        cls._app = rpcz.Application()
        cls._channel = cls._app.create_rpc_channel(cls._config.bind_str)
        cls._service = service_rpcz.SourceManagerService_Stub(cls._channel)
        cls._riakc = riak.RiakClient(
            protocol='pbc',
            host=cls._config.riak.host,
            pb_port=cls._config.riak.port)
        cls._sqlc = pymysql.connect(
            host=cls._config.maria_db.host,
            user=cls._config.maria_db.user,
            passwd=cls._config.maria_db.password,
            db=cls._config.maria_db.db,
            use_unicode=True,
            charset='utf8')

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.features = [
            {'name': 'timestamp_ms', 'type': source_pb2.Feature.REAL},
            {'name': 'return_stock1', 'type': source_pb2.Feature.REAL},
            {'name': 'logvol_stock1', 'type': source_pb2.Feature.REAL},
            {'name': 'sector_stock1', 'type': source_pb2.Feature.CATEGORICAL}
        ]
        self.records = [
            [103.31, 0.01, 97041., 'pharma'],
            [103.47, -0.31, 22440., 'telecomms'],
            [104.04, 0.41, 2145., 'tech'],
            [102.40, -1.14, 21049., 'utilities']
        ]

    def try_create_source(self, request):
        CreateSourceRequest = service_pb2.CreateSourceRequest
        CreateSourceResponse = service_pb2.CreateSourceResponse
        try:
            cur = SaltfishTests._sqlc.cursor()
            response = self._service.create_source(request,
                                                   deadline_ms=DEFAULT_DEADLINE)
            self.assertEqual(CreateSourceResponse.OK, response.status)
            if request.source.source_id:
                self.assertEqual(request.source.source_id, response.source_id)
            source_id = response.source_id
            cur.execute(
                'select source_id, user_id, source_schema, name, created'
                ' from sources where source_id = %s', (source_id, ))
            rows = cur.fetchall()
            self.assertEqual(1, len(rows))
            remote_user_id, remote_source_name = rows[0][1], rows[0][3]
            remote_schema = source_pb2.Schema()
            remote_schema.ParseFromString(rows[0][2])
            self.assertEqual(request.source.user_id, remote_user_id)
            self.assertEqual(request.source.name, remote_source_name)
            self.assertEqual(request.source.schema.features,
                             remote_schema.features)

            log.info('Sending a 2nd identical request to check'
                     'idempotentcy (source_id=%s)' % uuid.UUID(bytes=source_id))
            response2 = self._service.create_source(request,
                                                    deadline_ms=DEFAULT_DEADLINE)
            self.assertEqual(CreateSourceResponse.OK, response2.status)

            log.info('Trying same id, but different schema. ' \
                     'Error expected (source_id=%s)' % uuid.UUID(bytes=source_id))
            features = deepcopy(self.features)
            features[1]['type'] = source_pb2.Feature.CATEGORICAL
            request3 = make_create_source_req(source_id, request.source.name, features)
            response3 = self._service.create_source(request3,
                                                    deadline_ms=DEFAULT_DEADLINE)
            self.assertEqual(CreateSourceResponse.SOURCE_ID_ALREADY_EXISTS,
                             response3.status)
            log.info('Got error message: "%s"' % response3.msg)
        except rpcz.rpc.RpcDeadlineExceeded:
            log.error(FAILED_PREFIX + 'Deadline exceeded! The service did not respond in time')
            sys.exit(1)
        except google.protobuf.message.DecodeError:
            log.error(FAILED_PREFIX + 'Could not parse the source schema @ (b=%s, k=%s))'
                      % (SOURCES_META_BUCKET, source_id))
            sys.exit(2)

    ### Test functions ###
    def test_create_source_with_given_id(self):
        source_id = uuid.uuid4()
        source_name = ur"Some Test Source-123客\x00家話\\;"
        request = make_create_source_req(source_id.get_bytes(),
                                         source_name, self.features)
        log.info('Creating a new source with a given id (id=%s)..' % str(source_id))
        self.try_create_source(request)

    def test_create_source_with_no_id(self):
        source_name = u"Some Other Source 客家話 -- £$"
        request = make_create_source_req(name=source_name, features=self.features)
        log.info('Creating a new source without setting the id..')
        self.try_create_source(request)

    @unittest.skip("")
    def test_create_source_duplicate_feature_name(self):
        features = deepcopy(SaltfishTester.features)
        features.append(features[0])
        request = make_create_source_req(features=features)
        log.info("Creating source with duplicate feature name in schema. Error expected")
        try:
            response = self._service.create_source(request, deadline_ms=DEFAULT_DEADLINE)
            assert response.status == service_pb2.CreateSourceResponse.DUPLICATE_FEATURE_NAME
            log.info('Got error message: "%s"' % response.msg)
            log_success(TEST_PASSED)
        except rpcz.rpc.RpcDeadlineExceeded:
            log.error(FAILED_PREFIX + 'Deadline exceeded! The service did not respond in time')
            sys.exit(1)

    @unittest.skip("")
    def test_delete_source(self):
        create_request = make_create_source_req()
        delete_request = service_pb2.DeleteSourceRequest()
        log.info('Creating a source in order to delete it')
        try:
            create_response = self._service.create_source(create_request, deadline_ms=DEFAULT_DEADLINE)
            assert create_response.status == service_pb2.CreateSourceResponse.OK
            source_id = create_response.source_id

            remote_data = self._riakc.bucket(SOURCES_META_BUCKET).get(source_id)
            assert remote_data.encoded_data is not None

            log.info('Deleting the just created source with id=%s' % source_id)
            delete_request.source_id = source_id
            delete_response = self._service.delete_source(delete_request, deadline_ms=DEFAULT_DEADLINE)
            assert delete_response.status == service_pb2.DeleteSourceResponse.OK

            remote_data = self._riakc.bucket(SOURCES_META_BUCKET).get(source_id)
            assert remote_data.encoded_data is None
            log_success(TEST_PASSED)

            log.info('Making a 2nd identical delete_source call to check idempotentcy (source_id=%s)'
                     % source_id)
            delete_response = self._service.delete_source(delete_request, deadline_ms=DEFAULT_DEADLINE)
            assert delete_response.status == service_pb2.DeleteSourceResponse.OK
            log_success(TEST_PASSED)
        except rpcz.rpc.RpcDeadlineExceeded:
            log.error(FAILED_PREFIX + 'Deadline exceeded! The service did not respond in time')
            sys.exit(1)

    @unittest.skip("")
    def test_generate_id_multiple(self):
        N = 10
        log.info('Generating %d ids in one call to the service..' % N)
        req = service_pb2.GenerateIdRequest()
        req.count = N
        try:
            response = self._service.generate_id(req, deadline_ms=DEFAULT_DEADLINE)
            assert response.status == service_pb2.GenerateIdResponse.OK
            assert len(response.ids) == N
            log_success(TEST_PASSED)
        except rpcz.rpc.RpcDeadlineExceeded:
            log.error(FAILED_PREFIX + 'Deadline exceeded! The service did not respond in time')
            sys.exit(1)

    @unittest.skip("")
    def test_generate_id_error(self):
        log.info('Trying to generate 1000000 ids in one call. Error expected')
        req = service_pb2.GenerateIdRequest()
        req.count = 1000000
        try:
            response = self._service.generate_id(req, deadline_ms=DEFAULT_DEADLINE)
            assert response.status == service_pb2.GenerateIdResponse.COUNT_TOO_LARGE
            assert len(response.ids) == 0
            log.info('Got error message: "%s"' % response.msg)
            log_success(TEST_PASSED)
        except rpcz.rpc.RpcDeadlineExceeded:
            log.error(FAILED_PREFIX + 'Deadline exceeded! The service did not respond in time')
            sys.exit(1)

        # log.info('Calling the service %d times, generating 1 id per call' % N)

    @unittest.skip("")
    def test_put_records_with_new_source(self):
        try:
            log.info('Creating a source where the records will be inserted')
            create_source_req = make_create_source_req(features=SaltfishTester.features)
            create_source_resp = self._service.create_source(create_source_req, deadline_ms=DEFAULT_DEADLINE)
            assert create_source_resp.status == service_pb2.CreateSourceResponse.OK
            source_id = create_source_resp.source_id

            log.info('Inserting %d records into newly created source (id=%s)' %
                     (len(SaltfishTester.records), source_id))
            request = make_put_records_req(source_id, records=SaltfishTester.records)
            log.info("reals=%s; cats=%s" % (str(request.records[0].reals), str(request.records[0].cats)))
            response = self._service.put_records(request, deadline_ms=DEFAULT_DEADLINE)
            assert response.status == service_pb2.PutRecordsResponse.OK
            log.info(response.record_ids)
            log.info(SaltfishTester.records)
            assert len(response.record_ids) == len(SaltfishTester.records)
            bucket = SOURCES_DATA_BUCKET_ROOT + source_id + '/'
            for record_id in response.record_ids:
                     log.info('Checking record at b=%s / k=%s)' % (bucket, record_id))
                     remote_data = self._riakc.bucket(bucket).get(record_id)
                     assert remote_data.encoded_data is not None
            log.info('Got message: "%s"' % response.msg)
        except rpcz.rpc.RpcDeadlineExceeded:
            log.error(FAILED_PREFIX + 'Deadline exceeded! The service did not respond in time')
            sys.exit(1)

    @unittest.skip("")
    def test_put_records_with_nonexistent_source(self):
        try:
            source_id = "random_source_id_that_does_not_exist"
            log.info('Inserting %d records into a non-exsistent source (id=%s) Error expected' %
                     (len(SaltfishTester.records), source_id))
            request = make_put_records_req(source_id, records=SaltfishTester.records)
            response = self._service.put_records(request, deadline_ms=DEFAULT_DEADLINE)
            assert response.status == service_pb2.PutRecordsResponse.INVALID_SOURCE_ID
            assert len(response.record_ids) == 0
            log.info('Got error message: "%s"' % response.msg)
        except rpcz.rpc.RpcDeadlineExceeded:
            log.error(FAILED_PREFIX + 'Deadline exceeded! The service did not respond in time')
            sys.exit(1)

    @unittest.skip("")
    def test_rabbitmq_publisher(self):
        request = service_pb2.CreateSourceRequest()
        for f in self.features:
            new_feat = request.source.schema.features.add()
            new_feat.name = f['name']
            new_feat.feature_type = f['type']
        response = self._service.create_source(request,
                                               deadline_ms=DEFAULT_DEADLINE)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Saltfish Integration Tests")
    parser.add_argument('--conf', type=str, default=None, action='store',
                        help="Saltfish config file", metavar='FILE')
    options, args = parser.parse_known_args()
    if options.conf:
        with open(options.conf, 'r') as conf_file:
            config = config_pb2.Saltfish()
            google.protobuf.text_format.Merge(conf_file.read(), config)
            SaltfishTests.configure(config)
    unittest.main(argv=sys.argv[:1] + args)
