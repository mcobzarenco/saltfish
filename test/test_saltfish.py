#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import unittest
import sys
import time
import multiprocessing
import logging
import struct
from base64 import b64decode, b64encode
from collections import namedtuple
from copy import deepcopy
from os.path import join
from random import randint

import google.protobuf
import rpcz
from rpcz.rpc import RpcDeadlineExceeded
from rpcz import compiler
import pymysql
import riak
import uuid

from reinferio.store import SqlStore

from reinferio import core_pb2
from reinferio import saltfish_pb2
from reinferio.saltfish_pb2 import \
    CreateSourceRequest, CreateSourceResponse, \
    DeleteSourceRequest, DeleteSourceResponse, \
    GenerateIdRequest,   GenerateIdResponse, \
    PutRecordsRequest,   PutRecordsResponse, \
    GetSourcesRequest, GetSourcesResponse


PROTO_ROOT = join('..', 'src', 'proto')
DEFAULT_CONNECT = 'tcp://localhost:5555'
DEFAULT_RIAK_HOST = 'localhost'
DEFAULT_RIAK_PORT = 10017

SOURCE_ID_BYTES = 16

TEST_PASSED = '*** OK ***'
FAILED_PREFIX = 'TEST FAILED | '

DEFAULT_DEADLINE = 3

log = multiprocessing.log_to_stderr()
log.setLevel(logging.INFO)


compiler.generate_proto(join(PROTO_ROOT, 'config.proto'), '.')
import config_pb2


def make_create_source_req(
        source_id=None, user_id=None, name=None, features=None,
        private=True, frozen=False):
    features = features or []
    request = CreateSourceRequest()
    if source_id:  request.source.source_id = source_id
    if user_id:  request.source.user_id = user_id
    if name:  request.source.name = name
    for f in features:
        new_feat = request.source.schema.features.add()
        if 'name' in f:  new_feat.name = f['name']
        if 'type' in f:  new_feat.feature_type = f['type']
    request.source.private = private
    request.source.frozen = frozen
    return request


def filter_by_type(seq, typ):
    return filter(lambda val: isinstance(val, typ), seq)


def make_put_records_req(source_id, records=None, record_ids=None):
    records = records or []
    record_ids = record_ids or [None] * len(records)
    assert len(records) == len(record_ids)
    request = PutRecordsRequest()
    request.source_id = str(source_id)
    for i in xrange(len(records)):
        tagged = request.records.add()
        if record_ids[i]:
            tagged.record.record_id = record_ids[i]
        tagged.record.reals.extend(filter_by_type(records[i], float))
        tagged.record.cats.extend(filter_by_type(records[i], str))
    return request


def uuid2hex(uuid_str):
    return str(uuid.UUID(bytes=uuid_str)).replace('-', '')


def bytes_to_int64(int_str):
    return struct.unpack("<q", int_str)[0]


def valid_source_id(source_id):
    return isinstance(source_id, basestring) \
        and len(source_id) == SOURCE_ID_BYTES


def parse_schema(schema_str):
    schema = core_pb2.Schema()
    schema.ParseFromString(schema_str)
    return schema


class BinaryString(str):
    def encode(self, ignored):
        return self

ListSourcesRecord = namedtuple('ListSourcesRecord', [
    'source_id', 'user_id', 'source_schema', 'name', 'private',
    'frozen', 'created', 'username', 'email'])

class SaltfishTests(unittest.TestCase):
    _config = config_pb2.Saltfish()

    @classmethod
    def configure(cls, config):
        cls._config = config

    @classmethod
    def setUpClass(cls):
        cls._app = rpcz.Application()
        cls._channel = cls._app.create_rpc_channel(cls._config.bind_str)
        cls._service = saltfish_pb2.SourceManagerService_Stub(cls._channel)
        cls._riakc = riak.RiakClient(
            protocol='pbc',
            host=cls._config.riak.host,
            pb_port=cls._config.riak.port)
        cls._sql = SqlStore(
            host=cls._config.maria_db.host,
            port=cls._config.maria_db.port,
            user=cls._config.maria_db.user,
            passwd=cls._config.maria_db.password,
            db=cls._config.maria_db.db)

        # Create a test user:
        conn = cls._sql.check_out_connection()
        cur = conn.cursor()
        cur.execute('DELETE FROM sources')
        cur.execute('DELETE FROM users')
        cls._sql.check_in_connection(conn)

        user_id1 = cls._sql.create_user(
            'marius', 'marius@test.io', 'abcd', True)
        user_id2 = cls._sql.create_user(
            'reinfer', 'reinfer@test.io', 'abcde', False)
        cls._user_id1 = user_id1
        cls._user_id2 = user_id2

        assert(user_id1 is not None and user_id2 is not None)

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def get_sources(cls, source_id=None, user_id=None, username=None):
        SELECT_LIST_SOURCES = """
        SELECT source_id, user_id, source_schema, name, private, frozen,
         created, username, email FROM list_sources WHERE """
        args = [source_id, user_id, username]
        assert sum(map(lambda x: x is not None, args)) == 1
        print(args)

        conn = cls._sql.check_out_connection()
        assert conn is not None
        cur = conn.cursor()
        if source_id is not None:
            cur.execute(SELECT_LIST_SOURCES + 'source_id=%s', (source_id,))
        elif user_id is not None:
            cur.execute(SELECT_LIST_SOURCES + 'user_id=%s', (user_id,))
        elif username is not None:
            cur.execute(SELECT_LIST_SOURCES + 'username=%s', (username,))
        else:
            assert False
        sources = cur.fetchall()
        cls._sql.check_in_connection(conn)

        if len(sources) == 0:
            if source_id is not None:
                return None
            else:
                if cls._sql.get_user(
                        user_id=user_id, username=username) is None:
                    return None
        print(sources)
        return [ListSourcesRecord(
            source_id=source[0], user_id=source[1],
            source_schema=parse_schema(source[2]),
            name=source[3], private=source[4],
            frozen=source[5], created=source[6],
            username=source[7], email=source[8]) for source in sources]

    def setUp(self):
        self._features = [
            {'name': 'timestamp_ms', 'type': core_pb2.Feature.REAL},
            {'name': 'return', 'type': core_pb2.Feature.REAL},
            {'name': 'logvol', 'type': core_pb2.Feature.REAL},
            {'name': 'sector', 'type': core_pb2.Feature.CATEGORICAL}
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
        self.assertTrue(valid_source_id(source_id))

        cls = self.__class__
        remote = cls.get_sources(source_id=source_id)
        self.assertEqual(1, len(remote))
        remote = remote[0]

        # Also get the schema cached in Riak:
        cached = cls._riakc \
                    .bucket(cls._config.schemas_bucket) \
                    .get(BinaryString(source_id))
        self.assertNotEqual(
            cached.encoded_data, '',
            "The schema had not been cached in Riak b=%s k=%s (=source_id)" %
            (cls._config.schemas_bucket, uuid2hex(source_id)))

        cached_schema = core_pb2.Schema()
        cached_schema.ParseFromString(cached.encoded_data)

        self.assertEqual(remote.source_schema, cached_schema)
        if valid_source_id(request.source.source_id):
            self.assertEqual(request.source.source_id, remote.source_id)
        self.assertEqual(request.source.user_id, remote.user_id)
        self.assertEqual(request.source.name, remote.name)
        self.assertEqual(request.source.schema.features,
                         remote.source_schema.features)
        self.assertEqual(request.source.private, remote.private)
        self.assertEqual(request.source.frozen, remote.frozen)

    def try_create_source(self, request):
        response = self._service.create_source(
            request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateSourceResponse.OK, response.status)
        source_id = response.source_id

        self.verify_created_source(source_id, request)
        if request.source.source_id:
            self.assertEqual(request.source.source_id, response.source_id)
        else:
            request.source.source_id = source_id

        log.info('Sending a 2nd identical request to check '
                 'idempotency (source_id=%s)' % uuid2hex(source_id))
        response2 = self._service.create_source(
            request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateSourceResponse.OK, response2.status)
        self.verify_created_source(source_id, request)

        log.info('Trying same id, but different schema. ' \
                 'Error expected (source_id=%s)' % uuid2hex(source_id))
        features = deepcopy(self._features)
        features[1]['type'] = core_pb2.Feature.CATEGORICAL
        request3 = make_create_source_req(
            source_id, request.source.user_id, request.source.name, features)
        response3 = self._service.create_source(
            request3, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateSourceResponse.SOURCE_ID_ALREADY_EXISTS,
                         response3.status)
        log.info('Got error message: "%s"' % response3.msg)
        return response

    def try_delete_source(self, source_id):
        request = DeleteSourceRequest()
        request.source_id = source_id
        response = self._service.delete_source(
            request, deadline_ms=DEFAULT_DEADLINE)
        return response

    ### Test functions ###
    def test_create_source_with_given_source_id(self):
        source_id = uuid.uuid4()
        user_id = self.__class__._user_id1
        source_name = ur"Some Test Source-123客\x00家話\\;"
        request = make_create_source_req(
            source_id.get_bytes(), user_id, source_name, self._features)
        log.info('Creating a new source with a given id (id=%s)..' %
                 uuid2hex(source_id.get_bytes()))
        self.try_create_source(request)

    def test_create_source_with_no_source_id(self):
        user_id = self.__class__._user_id2
        source_name = u"Some Other Source 客家 話 -- £$"
        request = make_create_source_req(
            user_id=user_id, name=source_name, features=self._features)
        log.info('Creating a new source without setting the id..')
        self.try_create_source(request)

    def test_create_source_with_invalid_user_id(self):
        user_id = 1
        source_name = u"Source 123"
        request = make_create_source_req(
            user_id=user_id, name=source_name, features=self._features)
        log.info("Creating source with an invalid user_id. Error expected")
        response = self._service.create_source(
            request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateSourceResponse.INVALID_USER_ID, response.status)
        log.info('Got error message: "%s"' % response.msg)
        self.assertIsNone(self.__class__.get_sources(response.source_id))

    def test_create_source_duplicate_feature_name(self):
        user_id = self.__class__._user_id2
        source_id = uuid.uuid4().get_bytes()
        self._features.append(self._features[0])
        request = make_create_source_req(source_id, user_id,
                                         features=self._features)
        log.info("Creating source with duplicate feature name in schema." \
                 " Error expected")
        response = self._service.create_source(
            request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateSourceResponse.DUPLICATE_FEATURE_NAME,
                         response.status)
        log.info('Got error message: "%s"' % response.msg)
        self.assertIsNone(self.__class__.get_sources(source_id))

    def test_create_source_with_invalid_feature_type(self):
        user_id = self.__class__._user_id1
        features = self._features
        for feat in features:
            del feat['type']
        request = make_create_source_req(
            user_id=user_id, name="Source with invalid feature types",
            features=features)
        log.info("Creating source with invalid features in schema." \
                 " Error expected")

        response = self._service.create_source(
            request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateSourceResponse.INVALID_FEATURE_TYPE,
                         response.status)

    def test_create_source_two_sources_same_name(self):
        user_id = self.__class__._user_id1
        source_name = u"iris-dataset"
        source_id1 = uuid.uuid4().get_bytes()
        source_id2 = uuid.uuid4().get_bytes()

        # Only the source ids differ:
        request1 = make_create_source_req(
            source_id=source_id1, user_id=user_id,
            name=source_name, features=self._features)
        request2 = make_create_source_req(
            source_id=source_id2, user_id=user_id,
            name=source_name, features=self._features)

        log.info('Trying to create 2 sources with the same name for the same '
                 ' user. Error expected')
        self.try_create_source(request1)
        response = self._service.create_source(
            request2, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(
            CreateSourceResponse.DUPLICATE_SOURCE_NAME, response.status)
        log.info('Got error message: "%s"' % response.msg)

    def test_delete_source(self):
        user_id = self.__class__._user_id1
        create_request = make_create_source_req(
            user_id=user_id, features=self._features)
        delete_request = DeleteSourceRequest()
        log.info('Creating a source in order to delete it..')
        create_response = self._service.create_source(
            create_request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateSourceResponse.OK, create_response.status)
        source_id = create_response.source_id
        self.assertIsNotNone(self.__class__.get_sources(source_id))

        log.info('Deleting the just created source with id=%s'
                 % uuid2hex(source_id))
        delete_request.source_id = source_id
        delete_response = self._service.delete_source(
            delete_request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(DeleteSourceResponse.OK, delete_response.status)
        self.assertEqual(True, delete_response.updated)
        self.assertIsNone(self.__class__.get_sources(source_id))

        log.info('Making a 2nd identical delete_source call '
                 'to check idempotency (source_id=%s)'  % uuid2hex(source_id))
        delete_response = self._service.delete_source(
            delete_request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(DeleteSourceResponse.OK, delete_response.status)
        self.assertEqual(False, delete_response.updated)
        self.assertIsNone(self.__class__.get_sources(source_id))

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
        request = GenerateIdRequest()
        request.count = ID_COUNT
        response = self._service.generate_id(request,
                                             deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(GenerateIdResponse.OK, response.status)
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

    def _ensure_source(self, source_id=None, user_id=None,
                       name=None, features=None):
        create_req = make_create_source_req(
            source_id, user_id, name, features)
        create_resp = self._service.create_source(
            create_req, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateSourceResponse.OK, create_resp.status)
        return create_resp

    def test_put_records_with_new_source(self):
        log.info('Creating a source where the records will be inserted..')
        source_id = self._ensure_source(user_id=self.__class__._user_id1,
                                        features=self._features).source_id

        log.info('Inserting %d records into newly created source (id=%s)' %
                     (len(self._records), uuid2hex(source_id)))
        put_req = make_put_records_req(source_id, records=self._records)
        put_resp = self._service.put_records(put_req,
                                             deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(PutRecordsResponse.OK, put_resp.status)
        self.assertEqual(len(self._records), len(put_resp.record_ids))
        bucket = self.__class__._config.sources_data_bucket_prefix + \
                 str(uuid2hex(source_id))
        print(put_resp.record_ids)
        for i, record_id in enumerate(put_resp.record_ids):
            self.assertEqual(8, len(record_id))  # record_ids are int64_t
            log.info('Checking record at b=%s / k=%20s)' %
                     (bucket, bytes_to_int64(record_id)))
            remote = self._riakc.bucket(bucket).get(BinaryString(record_id))
            self.assertIsNotNone(remote.encoded_data)
            record = core_pb2.Record()
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

    def _make_some_sources(self, user_id, source_names):
        create_reqs = {}
        for i, name in enumerate(source_names):
            request = make_create_source_req(
                name=name, user_id=user_id, features=self._features,
                private=(i % 2 == 0), frozen=(i % 3 == 0))
            log.info('Creating source "%s" for user_id=%d'
                     % (name, user_id))
            response = self._service.create_source(
                request, deadline_ms=DEFAULT_DEADLINE)
            self.assertEqual(CreateSourceResponse.OK, response.status)
            self.verify_created_source(response.source_id, request)
            create_reqs[response.source_id] = request
        return create_reqs

    def test_get_sources_invalid_request(self):
        request = GetSourcesRequest()
        response = self._service.get_sources(
            request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(GetSourcesResponse.INVALID_REQUEST, response.status)

        request2 = GetSourcesRequest()
        request2.user_id = 1001
        request2.username = "test"
        response2 = self._service.get_sources(
            request2, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(GetSourcesResponse.INVALID_REQUEST, response2.status)

    def assert_get_sources_with_request(
            self, username, email, create_reqs, request):
        response = self._service.get_sources(
            request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(GetSourcesResponse.OK, response.status)
        self.assertEqual(len(create_reqs), len(response.sources_info))

        source_ids = set(create_reqs.keys())
        for source_info in response.sources_info:
            source = source_info.source
            self.assertIn(
                source.source_id, source_ids,
                "A source with id=%s was returned by list_sources"
                " for although it was not created in the test;"
                " remaining expected source_ids=%s"
                % (b64encode(source.source_id), map(b64encode, source_ids)))
            source_id = source.source_id
            request = create_reqs[source_id]
            self.verify_created_source(source_id, request)
            self.assertEqual(username, source_info.username)
            self.assertEqual(email, source_info.email)
            source_ids.remove(source.source_id)

            # Check that the same result is obtained using the source's id:
            req_by_id = GetSourcesRequest()
            req_by_id.source_id = source_id
            resp_by_id = self._service.get_sources(
                req_by_id, deadline_ms=DEFAULT_DEADLINE)
            self.assertEqual(GetSourcesResponse.OK, resp_by_id.status)
            self.assertEqual(1, len(resp_by_id.sources_info))
            self.assertEqual(source_info, resp_by_id.sources_info[0])

    def test_get_sources(self):
        username, email = 'list', 'list@test.io'
        user_id = self.__class__._sql.create_user(
            username, email, 'qwerty', False)
        source_names = ['src1', 'src2', 'src3'];
        create_reqs = self._make_some_sources(user_id, source_names)

        request = GetSourcesRequest()
        request.user_id = user_id
        self.assert_get_sources_with_request(
            username, email, create_reqs, request)

        request2 = GetSourcesRequest()
        request2.username = username
        self.assert_get_sources_with_request(
            username, email, create_reqs, request2)

    @unittest.skip("")
    def test_rabbitmq_publisher(self):
        request = CreateSourceRequest()
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
