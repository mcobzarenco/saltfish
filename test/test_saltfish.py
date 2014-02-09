#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import sys
import time
import argparse
import multiprocessing
import logging
import struct
from os.path import join
from copy import deepcopy
from random import randint

import google.protobuf
import rpcz
from rpcz.rpc import RpcDeadlineExceeded
from rpcz import compiler
import pymysql
import riak
import uuid


PROTO_ROOT = join('..', 'src', 'proto')
DEFAULT_CONNECT = 'tcp://localhost:5555'
DEFAULT_RIAK_HOST = 'localhost'
DEFAULT_RIAK_PORT = 10017

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
from service_pb2 import \
    CreateSourceRequest, CreateSourceResponse, \
    DeleteSourceRequest, DeleteSourceResponse, \
    GenerateIdRequest,   GenerateIdResponse, \
    PutRecordsRequest,   PutRecordsResponse

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


def make_put_records_req(source_id, records=None, record_ids=None):
    records = records or []
    record_ids = record_ids or [None] * len(records)
    assert len(records) == len(record_ids)
    request = service_pb2.PutRecordsRequest()
    request.source_id = str(source_id)
    for i in xrange(len(records)):
        tagged = request.records.add()
        if record_ids[i]:
            tagged.record.record_id = record_ids[i]
        tagged.record.reals.extend(filter_by_type(records[i], float))
        tagged.record.cats.extend(filter_by_type(records[i], str))
    return request


def uuid2hex(uuid_str):
    return str(uuid.UUID(bytes=uuid_str))


def bytes_to_int64(int_str):
    return struct.unpack("<q", int_str)[0]


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
            port=cls._config.maria_db.port,
            user=cls._config.maria_db.user,
            passwd=cls._config.maria_db.password,
            db=cls._config.maria_db.db,
            use_unicode=True,
            charset='utf8')
        cls._sqlc.autocommit(True)

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def fetch_source(cls, source_id):
        COLS = ['source_id', 'user_id', 'source_schema', 'name', 'created']
        cur = cls._sqlc.cursor()
        cur.execute(
            'select source_id, user_id, source_schema, name, created '
            'from sources where source_id = %s', (source_id, ))
        rows = cur.fetchall()
        return map(lambda x: dict(zip(COLS, x)), rows)

    def setUp(self):
        self._features = [
            {'name': 'timestamp_ms', 'type': source_pb2.Feature.REAL},
            {'name': 'return', 'type': source_pb2.Feature.REAL},
            {'name': 'logvol', 'type': source_pb2.Feature.REAL},
            {'name': 'sector', 'type': source_pb2.Feature.CATEGORICAL}
        ]
        self._records = [
            [103.31, 0.01, 97041., 'pharma'],
            [103.47, -0.31, 22440., 'telecomms'],
            [104.04, 0.41, 2145., 'tech'],
            [102.40, -1.14, 21049., 'utilities'],
            [2.41, 8.134, 1049., 'some bullshit'],
            [167.691, -13.14, 210349., 'some other bullshit'],
            [341023.4320, -1000.14, 210249.0432543, 'defence'],
            [5002.40, 0.0, 4919.2, 'energy'],
            [1002.40, -1.14, 6156., 'cons disc'],
        ]

    def verify_created_source(self, source_id, request):
        remote = SaltfishTests.fetch_source(source_id)
        self.assertEqual(1, len(remote))
        remote = remote[0]

        remote_schema = source_pb2.Schema()
        remote_schema.ParseFromString(remote['source_schema'])
        if request.source.source_id != '':
            self.assertEqual(request.source.source_id, remote['source_id'])
        self.assertEqual(request.source.user_id, remote['user_id'])
        self.assertEqual(request.source.name, remote['name'])
        self.assertEqual(request.source.schema.features,
                         remote_schema.features)

    def try_create_source(self, request):
        response = self._service.create_source(request,
                                               deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateSourceResponse.OK, response.status)
        if request.source.source_id:
            self.assertEqual(request.source.source_id, response.source_id)
        source_id = response.source_id
        self.verify_created_source(source_id, request)

        log.info('Sending a 2nd identical request to check '
                 'idempotency (source_id=%s)' % uuid2hex(source_id))
        response2 = self._service.create_source(request,
                                                deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateSourceResponse.OK, response2.status)
        self.verify_created_source(source_id, request)

        log.info('Trying same id, but different schema. ' \
                 'Error expected (source_id=%s)' % uuid2hex(source_id))
        features = deepcopy(self._features)
        features[1]['type'] = source_pb2.Feature.CATEGORICAL
        request3 = make_create_source_req(source_id, request.source.name, features)
        response3 = self._service.create_source(request3,
                                                deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateSourceResponse.SOURCE_ID_ALREADY_EXISTS,
                         response3.status)
        log.info('Got error message: "%s"' % response3.msg)

    def try_delete_source(self, source_id):
        request = service_pb2.DeleteSourceRequest()
        request.source_id = source_id
        response = self._service.delete_source(request,
                                               deadline_ms=DEFAULT_DEADLINE)
        return response

    ### Test functions ###
    def test_create_source_with_given_id(self):
        source_id = uuid.uuid4()
        source_name = ur"Some Test Source-123客\x00家話\\;"
        request = make_create_source_req(source_id.get_bytes(),
                                         source_name, self._features)
        log.info('Creating a new source with a given id (id=%s)..' %
                 uuid2hex(source_id.get_bytes()))
        self.try_create_source(request)

    def test_create_source_with_no_id(self):
        source_name = u"Some Other Source 客家話 -- £$"
        request = make_create_source_req(name=source_name, features=self._features)
        log.info('Creating a new source without setting the id..')
        self.try_create_source(request)

    def test_create_source_duplicate_feature_name(self):
        source_id = uuid.uuid4().get_bytes()
        self._features.append(self._features[0])
        request = make_create_source_req(source_id, features=self._features)
        log.info("Creating source with duplicate feature name in schema. Error expected")
        response = self._service.create_source(request,
                                               deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateSourceResponse.DUPLICATE_FEATURE_NAME, response.status)
        log.info('Got error message: "%s"' % response.msg)
        self.assertEqual(0, len(SaltfishTests.fetch_source(source_id)))

    def test_delete_source(self):
        create_request = make_create_source_req(features=self._features)
        delete_request = service_pb2.DeleteSourceRequest()
        log.info('Creating a source in order to delete it..')
        create_response = self._service.create_source(
            create_request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateSourceResponse.OK, create_response.status)
        source_id = create_response.source_id
        self.assertEqual(1, len(SaltfishTests.fetch_source(source_id)))

        log.info('Deleting the just created source with id=%s' % uuid2hex(source_id))
        delete_request.source_id = source_id
        delete_response = self._service.delete_source(delete_request,
                                                      deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(DeleteSourceResponse.OK, delete_response.status)
        self.assertEqual(True, delete_response.updated)
        self.assertEqual(0, len(SaltfishTests.fetch_source(source_id)))

        log.info('Making a 2nd identical delete_source call '
                 'to check idempotency (source_id=%s)'  % uuid2hex(source_id))
        delete_response = self._service.delete_source(
            delete_request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(DeleteSourceResponse.OK, delete_response.status)
        self.assertEqual(False, delete_response.updated)
        self.assertEqual(0, len(SaltfishTests.fetch_source(source_id)))

    def test_delete_source_invalid_ids(self):
        log.info('Sending delete_request with invalid ids. Errors expected')
        response = self.try_delete_source("invalid\x00\x01\x02")
        self.assertEqual(DeleteSourceResponse.INVALID_SOURCE_ID,
                         response.status)
        log.info('id too short - error message: "%s"' % response.msg)

        response2 = self.try_delete_source("")
        self.assertEqual(DeleteSourceResponse.INVALID_SOURCE_ID,
                         response2.status)
        log.info('id empty str - error message: "%s"' % response2.msg)

    def test_generate_id(self):
        ID_COUNT = 10
        log.info('Generating %d ids in one call to the service..' % ID_COUNT)
        request = service_pb2.GenerateIdRequest()
        request.count = ID_COUNT
        response = self._service.generate_id(request,
                                             deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(service_pb2.GenerateIdResponse.OK, response.status)
        self.assertEqual(ID_COUNT, len(response.ids))

    def test_generate_id_error(self):
        ID_COUNT = 1000000
        log.info('Trying to generate %d ids in one call. Error expected' % ID_COUNT)
        request = GenerateIdRequest()
        request.count = ID_COUNT

        response = self._service.generate_id(request,
                                             deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(GenerateIdResponse.COUNT_TOO_LARGE,
                         response.status)
        self.assertEqual(0, len(response.ids))
        log.info('Got error message: "%s"' % response.msg)

    def _ensure_source(self, source_id=None, name=None, features=None):
        create_req = make_create_source_req(source_id, name, features)
        create_resp = self._service.create_source(create_req,
                                                  deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateSourceResponse.OK, create_resp.status)
        return create_resp

    def test_put_records_with_new_source(self):
        log.info('Creating a source where the records will be inserted..')
        source_id = self._ensure_source(features=self._features).source_id

        log.info('Inserting %d records into newly created source (id=%s)' %
                     (len(self._records), uuid2hex(source_id)))
        put_req = make_put_records_req(source_id, records=self._records)
        put_resp = self._service.put_records(put_req,
                                             deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(PutRecordsResponse.OK, put_resp.status)
        self.assertEqual(len(self._records), len(put_resp.record_ids))
        bucket = SOURCES_DATA_BUCKET_ROOT + str(uuid2hex(source_id))
        print(put_resp.record_ids)
        for i, record_id in enumerate(put_resp.record_ids):
            self.assertEqual(8, len(record_id))  # record_ids are int64_t
            log.info('Checking record at b=%s / k=%20s)' %
                     (bucket, bytes_to_int64(record_id)))
            remote = self._riakc.bucket(bucket).get(BinaryString(record_id))
            self.assertIsNotNone(remote.encoded_data)
            record = source_pb2.Record()
            record.ParseFromString(remote.encoded_data)
            self.assertEqual(put_req.records[i].record, record)

    def test_put_records_with_nonexistent_source(self):
        source_id = uuid.uuid4().get_bytes()
        log.info('Inserting %d records into a non-exsistent source (id=%s)' \
                 ' Error expected' % (len(self._records), uuid2hex(source_id)))
        request = make_put_records_req(source_id, records=self._records)
        response = self._service.put_records(request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(PutRecordsResponse.INVALID_SOURCE_ID, response.status)
        self.assertEqual(0, len(response.record_ids))
        log.info('Got error message: "%s"' % response.msg)

    @unittest.skip("")
    def test_rabbitmq_publisher(self):
        request = service_pb2.CreateSourceRequest()
        for f in self._features:
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
