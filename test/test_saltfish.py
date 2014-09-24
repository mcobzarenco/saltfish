#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import logging
import multiprocessing
import os
import struct
import sys
import time
import unittest
from base64 import urlsafe_b64decode as b64decode, \
    urlsafe_b64encode as b64encode
from collections import namedtuple
from copy import deepcopy
from os.path import join
from random import randint

import google.protobuf
import pymysql
import riak
import rpcz
from rpcz.rpc import RpcDeadlineExceeded
from rpcz import compiler

from reinferio.store import SqlStore
from reinferio import core_pb2
from reinferio import saltfish_pb2
from reinferio.saltfish_pb2 import \
    CreateDatasetRequest, CreateDatasetResponse, \
    DeleteDatasetRequest, DeleteDatasetResponse, \
    GenerateIdRequest,   GenerateIdResponse, \
    PutRecordsRequest,   PutRecordsResponse, \
    GetDatasetsRequest, GetDatasetsResponse


PROTO_ROOT = join('..', 'src', 'proto')
DEFAULT_CONNECT = 'tcp://localhost:5555'
DEFAULT_RIAK_HOST = 'localhost'
DEFAULT_RIAK_PORT = 8087

DATASET_ID_BYTES = 24
RANDOM_INDEX = 'randomindex_int'

TEST_PASSED = '*** OK ***'
FAILED_PREFIX = 'TEST FAILED | '

DEFAULT_DEADLINE = 3

log = multiprocessing.log_to_stderr()
log.setLevel(logging.INFO)


compiler.generate_proto(join(PROTO_ROOT, 'config.proto'), '.')
import config_pb2


def make_create_dataset_req(
        dataset_id=None, user_id=None, name=None, features=None,
        private=True, frozen=False):
    features = features or []
    request = CreateDatasetRequest()
    if dataset_id:  request.dataset.id = dataset_id
    if user_id:  request.dataset.user_id = user_id
    if name:  request.dataset.name = name
    for f in features:
        new_feat = request.dataset.schema.features.add()
        if 'name' in f:  new_feat.name = f['name']
        if 'type' in f:  new_feat.type = f['type']
    request.dataset.private = private
    request.dataset.frozen = frozen
    return request


def filter_by_type(seq, typ):
    return filter(lambda val: isinstance(val, typ), seq)


def make_put_records_req(dataset_id, records=None, record_ids=None):
    records = records or []
    record_ids = record_ids or [None] * len(records)
    assert len(records) == len(record_ids)
    request = PutRecordsRequest()
    request.dataset_id = str(dataset_id)
    for i in xrange(len(records)):
        tagged = request.records.add()
        if record_ids[i]:
            tagged.record.record_id = record_ids[i]
        tagged.record.numericals.extend(filter_by_type(records[i], float))
        tagged.record.categoricals.extend(filter_by_type(records[i], str))
    return request


def bytes_to_int64(int_str):
    return struct.unpack("<q", int_str)[0]


def valid_dataset_id(dataset_id):
    return isinstance(dataset_id, basestring) \
        and len(dataset_id) == DATASET_ID_BYTES


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
        cls._service = saltfish_pb2.DatasetStore_Stub(cls._channel)
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
    def get_datasets(cls, dataset_id=None, user_id=None, username=None):
        SELECT_LIST_SOURCES = """
        SELECT source_id, user_id, source_schema, name, private, frozen,
         created, username, email FROM list_sources WHERE """
        args = [dataset_id, user_id, username]
        assert sum(map(lambda x: x is not None, args)) == 1

        conn = cls._sql.check_out_connection()
        assert conn is not None
        cur = conn.cursor()
        if dataset_id is not None:
            cur.execute(SELECT_LIST_SOURCES + 'source_id=%s', (dataset_id,))
        elif user_id is not None:
            cur.execute(SELECT_LIST_SOURCES + 'user_id=%s', (user_id,))
        elif username is not None:
            cur.execute(SELECT_LIST_SOURCES + 'username=%s', (username,))
        else:
            assert False
        sources = cur.fetchall()
        cls._sql.check_in_connection(conn)

        if len(sources) == 0:
            if dataset_id is not None:
                return None
            else:
                if cls._sql.get_user(
                        user_id=user_id, username=username) is None:
                    return None

        return [ListSourcesRecord(
            source_id=source[0], user_id=source[1],
            source_schema=parse_schema(source[2]),
            name=source[3], private=source[4],
            frozen=source[5], created=source[6],
            username=source[7], email=source[8]) for source in sources]

    def setUp(self):
        self._features = [
            {'name': 'timestamp_ms', 'type': core_pb2.Feature.NUMERICAL},
            {'name': 'return', 'type': core_pb2.Feature.NUMERICAL},
            {'name': 'logvol', 'type': core_pb2.Feature.NUMERICAL},
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

    def verify_created_dataset(self, dataset_id, request):
        self.assertTrue(valid_dataset_id(dataset_id))

        cls = self.__class__
        remote = cls.get_datasets(dataset_id=dataset_id)
        self.assertIsNotNone(remote)
        self.assertEqual(1, len(remote))
        remote = remote[0]

        # Also get the schema cached in Riak:
        cached = cls._riakc \
                    .bucket(cls._config.schemas_bucket) \
                    .get(BinaryString(dataset_id))
        self.assertNotEqual(
            cached.encoded_data, '',
            "The schema had not been cached in Riak b=%s k=%s (=dataset_id)" %
            (cls._config.schemas_bucket, b64encode(dataset_id)))

        cached_schema = core_pb2.Schema()
        cached_schema.ParseFromString(cached.encoded_data)

        self.assertEqual(remote.source_schema, cached_schema)
        if valid_dataset_id(request.dataset.id):
            self.assertEqual(request.dataset.id, remote.source_id)
        self.assertEqual(request.dataset.user_id, remote.user_id)
        self.assertEqual(request.dataset.name, remote.name)
        self.assertEqual(request.dataset.schema.features,
                         remote.source_schema.features)
        self.assertEqual(request.dataset.private, remote.private)
        self.assertEqual(request.dataset.frozen, remote.frozen)

    def try_create_dataset(self, request):
        response = self._service.create_dataset(
            request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateDatasetResponse.OK, response.status)
        dataset_id = response.dataset_id

        self.verify_created_dataset(dataset_id, request)
        if request.dataset.id:
            self.assertEqual(request.dataset.id, response.dataset_id)
        else:
            request.dataset.id = dataset_id

        log.info('Sending a 2nd identical request to check '
                 'idempotency (dataset_id=%s)' % b64encode(dataset_id))
        response2 = self._service.create_dataset(
            request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateDatasetResponse.OK, response2.status)
        self.verify_created_dataset(dataset_id, request)

        log.info('Trying same id, but different schema. ' \
                 'Error expected (dataset_id=%s)' % b64encode(dataset_id))
        features = deepcopy(self._features)
        features[1]['type'] = core_pb2.Feature.CATEGORICAL
        request3 = make_create_dataset_req(
            dataset_id, request.dataset.user_id, request.dataset.name, features)
        response3 = self._service.create_dataset(
            request3, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateDatasetResponse.DATASET_ID_ALREADY_EXISTS,
                         response3.status)
        log.info('Got error message: "%s"' % response3.msg)
        return response

    def try_delete_dataset(self, dataset_id):
        request = DeleteDatasetRequest()
        request.dataset_id = dataset_id
        response = self._service.delete_dataset(
            request, deadline_ms=DEFAULT_DEADLINE)
        return response

    ### Test functions ###
    def test_create_dataset_with_given_id(self):
        dataset_id = os.urandom(DATASET_ID_BYTES)
        user_id = self.__class__._user_id1
        dataset_name = ur"Some Test Dataset-123客\x00家話\\;"
        request = make_create_dataset_req(
            dataset_id, user_id, dataset_name, self._features)
        log.info('Creating a new dataset with a given id (id=%s)..' %
                 b64encode(dataset_id))
        self.try_create_dataset(request)

    def test_create_dataset_without_given_id(self):
        user_id = self.__class__._user_id2
        dataset_name = u"Some Other Dataset 客家 話 -- £$"
        request = make_create_dataset_req(
            user_id=user_id, name=dataset_name, features=self._features)
        log.info('Creating a new dataset without setting the id..')
        self.try_create_dataset(request)

    def test_create_dataset_with_invalid_user_id(self):
        user_id = 1
        dataset_name = u"Dataset 123"
        request = make_create_dataset_req(
            user_id=user_id, name=dataset_name, features=self._features)
        log.info("Creating dataset with an invalid user_id. Error expected")
        response = self._service.create_dataset(
            request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateDatasetResponse.INVALID_USER_ID, response.status)
        log.info('Got error message: "%s"' % response.msg)
        self.assertIsNone(self.__class__.get_datasets(response.dataset_id))

    def test_create_dataset_duplicate_feature_name(self):
        user_id = self.__class__._user_id2
        dataset_id = os.urandom(DATASET_ID_BYTES)
        self._features.append(self._features[0])
        request = make_create_dataset_req(
            dataset_id, user_id, features=self._features)
        log.info("Creating dataset with duplicate feature name in schema." \
                 " Error expected")
        response = self._service.create_dataset(
            request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateDatasetResponse.DUPLICATE_FEATURE_NAME,
                         response.status)
        log.info('Got error message: "%s"' % response.msg)
        self.assertIsNone(self.__class__.get_datasets(dataset_id))

    def test_create_dataset_with_invalid_feature_type(self):
        user_id = self.__class__._user_id1
        features = self._features
        for feat in features:
            del feat['type']
        request = make_create_dataset_req(
            user_id=user_id, name="Dataset with invalid feature types",
            features=features)
        log.info("Creating dataset with invalid features in schema." \
                 " Error expected")

        response = self._service.create_dataset(
            request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateDatasetResponse.INVALID_FEATURE_TYPE,
                         response.status)

    def test_create_dataset_two_datasets_same_name(self):
        user_id = self.__class__._user_id1
        dataset_name = u"iris-dataset"
        dataset_id1 = os.urandom(DATASET_ID_BYTES)
        dataset_id2 = os.urandom(DATASET_ID_BYTES)

        # Only the dataset ids differ:
        request1 = make_create_dataset_req(
            dataset_id=dataset_id1, user_id=user_id,
            name=dataset_name, features=self._features)
        request2 = make_create_dataset_req(
            dataset_id=dataset_id2, user_id=user_id,
            name=dataset_name, features=self._features)

        log.info('Trying to create 2 datasets with the same name for the same '
                 ' user. Error expected')
        self.try_create_dataset(request1)
        response = self._service.create_dataset(
            request2, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(
            CreateDatasetResponse.DUPLICATE_DATASET_NAME, response.status)
        log.info('Got error message: "%s"' % response.msg)

    def test_delete_dataset(self):
        user_id = self.__class__._user_id1
        create_request = make_create_dataset_req(
            user_id=user_id, features=self._features)
        delete_request = DeleteDatasetRequest()
        log.info('Creating a dataset in order to delete it..')
        create_response = self._service.create_dataset(
            create_request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateDatasetResponse.OK, create_response.status)
        dataset_id = create_response.dataset_id
        self.assertIsNotNone(self.__class__.get_datasets(dataset_id))

        log.info('Deleting the just created dataset with id=%s'
                 % b64encode(dataset_id))
        delete_request.dataset_id = dataset_id
        delete_response = self._service.delete_dataset(
            delete_request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(DeleteDatasetResponse.OK, delete_response.status)
        self.assertEqual(True, delete_response.updated)
        self.assertIsNone(self.__class__.get_datasets(dataset_id))

        log.info('Making a 2nd identical delete_dataset call '
                 'to check idempotency (dataset_id=%s)'  % b64encode(dataset_id))
        delete_response = self._service.delete_dataset(
            delete_request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(DeleteDatasetResponse.OK, delete_response.status)
        self.assertEqual(False, delete_response.updated)
        self.assertIsNone(self.__class__.get_datasets(dataset_id))

    def test_delete_dataset_invalid_ids(self):
        log.info('Sending delete_request with invalid ids. Errors expected')
        response = self.try_delete_dataset("invalid\x00\x01\x02")
        self.assertEqual(DeleteDatasetResponse.INVALID_DATASET_ID,
                         response.status)
        log.info('id too short - error message: "%s"' % response.msg)

        response2 = self.try_delete_dataset("")
        self.assertEqual(DeleteDatasetResponse.INVALID_DATASET_ID,
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

    def _ensure_dataset(self, dataset_id=None, user_id=None,
                       name=None, features=None):
        create_req = make_create_dataset_req(
            dataset_id, user_id, name, features)
        create_resp = self._service.create_dataset(
            create_req, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(CreateDatasetResponse.OK, create_resp.status)
        return create_resp

    def _check_record_indexes(self, indexes):
        config = self.__class__._config
        self.assertIn(RANDOM_INDEX, map(lambda i: i[0], indexes))
        random_index_value = filter(
            lambda index: index[0] == RANDOM_INDEX, indexes)[0][1]
        self.assertLess(random_index_value, config.max_random_index)

    def test_put_records_with_new_dataset(self):
        log.info('Creating a dataset where the records will be inserted..')
        response = self._ensure_dataset(
            user_id=self.__class__._user_id1, features=self._features)
        dataset_id = response.dataset_id

        log.info('Inserting %d records into newly created dataset (id=%s)' %
                     (len(self._records), b64encode(dataset_id)))
        put_req = make_put_records_req(dataset_id, records=self._records)
        put_resp = self._service.put_records(put_req,
                                             deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(PutRecordsResponse.OK, put_resp.status)
        self.assertEqual(len(self._records), len(put_resp.record_ids))
        bucket = self.__class__._config.records_bucket_prefix + \
                 b64encode(dataset_id)

        for i, record_id in enumerate(put_resp.record_ids):
            self.assertEqual(8, len(record_id))  # record_ids are int64_t
            log.info('Checking record at b=%s / k=%20s)' %
                     (bucket, bytes_to_int64(record_id)))
            remote = self._riakc.bucket(bucket).get(BinaryString(record_id))
            self.assertTrue(remote.exists)
            self.assertIsNotNone(remote.encoded_data)
            self._check_record_indexes(remote.indexes)


            record = core_pb2.Record()
            record.ParseFromString(remote.encoded_data)
            self.assertEqual(put_req.records[i].record, record)

    def test_put_records_with_nonexistent_dataset(self):
        dataset_id = os.urandom(DATASET_ID_BYTES)
        log.info('Inserting %d records into a non-exsistent dataset (id=%s)' \
                 ' Error expected' % (len(self._records), b64encode(dataset_id)))
        request = make_put_records_req(dataset_id, records=self._records)
        response = self._service.put_records(request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(PutRecordsResponse.INVALID_DATASET_ID, response.status)
        self.assertEqual(0, len(response.record_ids))
        log.info('Got error message: "%s"' % response.msg)

    def _make_some_datasets(self, user_id, dataset_names):
        create_reqs = {}
        for i, name in enumerate(dataset_names):
            request = make_create_dataset_req(
                name=name, user_id=user_id, features=self._features,
                private=(i % 2 == 0), frozen=(i % 3 == 0))
            log.info('Creating dataset "%s" for user_id=%d'
                     % (name, user_id))
            response = self._service.create_dataset(
                request, deadline_ms=DEFAULT_DEADLINE)
            self.assertEqual(CreateDatasetResponse.OK, response.status)
            self.verify_created_dataset(response.dataset_id, request)
            create_reqs[response.dataset_id] = request
        return create_reqs

    def test_get_datasets_invalid_request(self):
        request = GetDatasetsRequest()
        response = self._service.get_datasets(
            request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(GetDatasetsResponse.INVALID_REQUEST, response.status)

        request2 = GetDatasetsRequest()
        request2.user_id = 1001
        request2.username = "test"
        response2 = self._service.get_datasets(
            request2, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(GetDatasetsResponse.INVALID_REQUEST, response2.status)

    def assert_get_datasets_with_request(
            self, username, email, create_reqs, request):
        response = self._service.get_datasets(
            request, deadline_ms=DEFAULT_DEADLINE)
        self.assertEqual(GetDatasetsResponse.OK, response.status)
        self.assertEqual(len(create_reqs), len(response.datasets))

        dataset_ids = set(create_reqs.keys())
        for dataset_detail in response.datasets:
            dataset = dataset_detail.dataset
            self.assertIn(
                dataset.id, dataset_ids,
                "A dataset with id=%s was returned by get_datasets"
                " although it was not created in the test;"
                " remaining expected dataset_ids=%s"
                % (b64encode(dataset.id), map(b64encode, dataset_ids)))
            dataset_id = dataset.id
            request = create_reqs[dataset_id]
            self.verify_created_dataset(dataset_id, request)
            self.assertEqual(username, dataset_detail.username)
            self.assertEqual(email, dataset_detail.email)
            dataset_ids.remove(dataset.id)

            # Check that the same result is obtained using the dataset id:
            req_by_id = GetDatasetsRequest()
            req_by_id.dataset_id = dataset_id
            resp_by_id = self._service.get_datasets(
                req_by_id, deadline_ms=DEFAULT_DEADLINE)
            self.assertEqual(GetDatasetsResponse.OK, resp_by_id.status)
            self.assertEqual(1, len(resp_by_id.datasets))
            self.assertEqual(dataset_detail, resp_by_id.datasets[0])

    def test_get_datasets(self):
        username, email = 'list', 'list@test.io'
        user_id = self.__class__._sql.create_user(
            username, email, 'qwerty', False)
        dataset_names = ['src1', 'src2', 'src3'];
        create_reqs = self._make_some_datasets(user_id, dataset_names)

        request = GetDatasetsRequest()
        request.user_id = user_id
        self.assert_get_datasets_with_request(
            username, email, create_reqs, request)

        request2 = GetDatasetsRequest()
        request2.username = username
        self.assert_get_datasets_with_request(
            username, email, create_reqs, request2)


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
