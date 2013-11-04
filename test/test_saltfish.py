#!/usr/bin/env python
import sys
import argparse
import multiprocessing
from os.path import join
from copy import deepcopy
from random import randint

import google.protobuf
import rpcz
from rpcz.rpc import RpcDeadlineExceeded
from rpcz import compiler
import riak

from test_logging import *

PROTO_ROOT = join('..', 'src', 'proto')
DEFAULT_CONNECT = 'tcp://localhost:5555'
DEFAULT_RIAK_HOST = 'localhost'
DEFAULT_RIAK_PORT = 10017

SOURCES_META_BUCKET = '/ml/sources/schemas/'
SOURCES_DATA_BUCKET_ROOT = '/ml/sources/data/'

TEST_PASSED = '*** OK ***'
FAILED_PREFIX = 'TEST FAILED | '

DEFAULT_DEADLINE = 2


compiler.generate_proto(join(PROTO_ROOT, 'source.proto'), '.')
compiler.generate_proto(join(PROTO_ROOT, 'service.proto'), '.')
compiler.generate_proto(join(PROTO_ROOT, 'service.proto'), '.',
                        with_plugin='python_rpcz', suffix='_rpcz.py')
import source_pb2
import service_pb2
import service_rpcz


def make_create_source_req(source_id=None, features=None):
    features = features or []
    request = service_pb2.CreateSourceRequest()
    if source_id:
        request.source_id = source_id
    for f in features:
        new_feat = request.schema.features.add()
        new_feat.name = f['name']
        new_feat.feature_type = f['type']
    return request


def filter_by_type(seq, typ):
    return filter(lambda val: isinstance(val, typ), seq)


def make_put_records_req(source_id, record_ids=None, records=None):
    records = records or []
    request = service_pb2.PutRecordsRequest()
    request.source_id = source_id
    if record_ids:
        request.record_ids.extend(record_ids)
    for record in records:
        new_record = request.records.add()
        new_record.reals.extend(filter_by_type(record, float))
        new_record.cats.extend(filter_by_type(record, str))
    return request


class SaltfishTester(object):
    features = [
        {'name': 'timestamp_ms', 'type': source_pb2.Feature.REAL},
        {'name': 'return_stock1', 'type': source_pb2.Feature.REAL},
        {'name': 'logvol_stock1', 'type': source_pb2.Feature.REAL},
        {'name': 'sector_stock1', 'type': source_pb2.Feature.CATEGORICAL}
    ]

    records = [
        [103.31, 0.01, 97041., 'pharma'],
        [103.47, -0.31, 22440., 'telecomms'],
        [104.04, 0.41, 2145., 'tech'],
        [102.40, -1.14, 21049., 'utilities']
    ]

    def __init__(self, connect_str, riak_host, riak_port):
        self._app = rpcz.Application()
        self._channel = self._app.create_rpc_channel(connect_str)
        self._service = service_rpcz.SourceManager_Stub(self._channel)
        self._riakc = riak.RiakClient(protocol='pbc', host=riak_host, pb_port=riak_port)

    def run_tests(self):
        results = {}
        n_tests = 0
        for t in filter(lambda x: x.startswith('test'), dir(self)):
            n_tests += 1
            log_run_test('Running %s' % t)
            results[t] = getattr(self, t)()
        log_success('SUCCESS | All %d test functions passed' % n_tests)
        return results

    def do_test_create_source(self, source_id=None):
        if source_id:
            log_info('Creating a new source without setting the id..')
        else:
            log_info('Creating a new source with a given id (id=%s)..' % source_id)

        try:
            req = make_create_source_req(source_id, SaltfishTester.features)
            response = self._service.create_source(req, deadline_ms=DEFAULT_DEADLINE)
            assert response.status == service_pb2.CreateSourceResponse.OK
            if source_id:
                assert response.source_id == source_id
            else:
                source_id = response.source_id

            remote_data = self._riakc.bucket(SOURCES_META_BUCKET).get(source_id)
            assert remote_data.encoded_data is not None
            remote_schema = source_pb2.Schema()
            remote_schema.ParseFromString(remote_data.encoded_data)
            remote_features = map(lambda x: {
                'name': x.name,
                'type': x.feature_type
            }, remote_schema.features)
            assert remote_features == SaltfishTester.features
            log_success(TEST_PASSED)

            log_info('Sending a 2nd identical request to check idempotentcy (source_id=%s)'
                     % source_id)
            response = self._service.create_source(req, deadline_ms=DEFAULT_DEADLINE)
            assert response.status == service_pb2.CreateSourceResponse.OK
            log_success(TEST_PASSED)

            log_info('Trying same id, but different schema. ' \
                     'Error expected (source_id=%s)' % source_id)
            features = deepcopy(SaltfishTester.features)
            features[1]['type'] = source_pb2.Feature.CATEGORICAL
            req2 = make_create_source_req(source_id, features)
            response = self._service.create_source(req2, deadline_ms=DEFAULT_DEADLINE)
            assert response.status == service_pb2.CreateSourceResponse.ERROR
            log_info('Got error message: "%s"' % response.msg)
            log_success(TEST_PASSED)
        except rpcz.rpc.RpcDeadlineExceeded:
            log_error(FAILED_PREFIX + 'Deadline exceeded! The service did not respond in time')
            sys.exit(1)
        except google.protobuf.message.DecodeError:
            log_error(FAILED_PREFIX + 'Could not parse the source schema @ (b=%s, k=%s))'
                      % (SOURCES_META_BUCKET, source_id))
            sys.exit(1)

    ### Test functions ###

    def test_create_source_with_given_id(self):
        source_id = 'test_' + str(randint(0, 10000000))
        self.do_test_create_source(source_id)

    def test_create_source_with_no_id(self):
        self.do_test_create_source()

    def test_create_source_duplicate_feature_name(self):
        features = deepcopy(SaltfishTester.features)
        features.append(features[0])
        request = make_create_source_req(features=features)
        log_info("Creating source with duplicate feature name in schema. Error expected")
        try:
            response = self._service.create_source(request, deadline_ms=DEFAULT_DEADLINE)
            assert response.status == service_pb2.CreateSourceResponse.ERROR
            log_info('Got error message: "%s"' % response.msg)
            log_success(TEST_PASSED)
        except rpcz.rpc.RpcDeadlineExceeded:
            log_error(FAILED_PREFIX + 'Deadline exceeded! The service did not respond in time')
            sys.exit(1)

    def test_delete_source(self):
        create_request = make_create_source_req()
        delete_request = service_pb2.DeleteSourceRequest()
        log_info('Creating a source in order to delete it')
        try:
            create_response = self._service.create_source(create_request, deadline_ms=DEFAULT_DEADLINE)
            assert create_response.status == service_pb2.CreateSourceResponse.OK
            source_id = create_response.source_id

            remote_data = self._riakc.bucket(SOURCES_META_BUCKET).get(source_id)
            assert remote_data.encoded_data is not None

            log_info('Deleting the just created source with id=%s' % source_id)
            delete_request.source_id = source_id
            delete_response = self._service.delete_source(delete_request, deadline_ms=DEFAULT_DEADLINE)
            assert delete_response.status == service_pb2.DeleteSourceResponse.OK

            remote_data = self._riakc.bucket(SOURCES_META_BUCKET).get(source_id)
            assert remote_data.encoded_data is None
            log_success(TEST_PASSED)

            log_info('Making a 2nd identical delete_source call to check idempotentcy (source_id=%s)' % source_id)
            delete_response = self._service.delete_source(delete_request, deadline_ms=DEFAULT_DEADLINE)
            assert delete_response.status == service_pb2.DeleteSourceResponse.OK
            log_success(TEST_PASSED)
        except rpcz.rpc.RpcDeadlineExceeded:
            log_error(FAILED_PREFIX + 'Deadline exceeded! The service did not respond in time')
            sys.exit(1)

    def test_generate_id_multiple(self):
        N = 10
        log_info('Generating %d ids in one call to the service..' % N)
        req = service_pb2.GenerateIdRequest()
        req.count = N
        try:
            response = self._service.generate_id(req, deadline_ms=DEFAULT_DEADLINE)
            assert response.status == service_pb2.GenerateIdResponse.OK
            assert len(response.ids) == N
            log_success(TEST_PASSED)
        except rpcz.rpc.RpcDeadlineExceeded:
            log_error(FAILED_PREFIX + 'Deadline exceeded! The service did not respond in time')
            sys.exit(1)

    def test_generate_id_error(self):
        log_info('Trying to generate 1000000 ids in one call. Error expected')
        req = service_pb2.GenerateIdRequest()
        req.count = 1000000
        try:
            response = self._service.generate_id(req, deadline_ms=DEFAULT_DEADLINE)
            assert response.status == service_pb2.GenerateIdResponse.ERROR
            assert len(response.ids) == 0
            log_info('Got error message: "%s"' % response.msg)
            log_success(TEST_PASSED)
        except rpcz.rpc.RpcDeadlineExceeded:
            log_error(FAILED_PREFIX + 'Deadline exceeded! The service did not respond in time')
            sys.exit(1)

        # log_info('Calling the service %d times, generating 1 id per call' % N)

    def test_put_records_with_new_source(self):
        try:
            log_info('Creating a source where the records will be inserted')
            create_source_req = make_create_source_req(features=SaltfishTester.features)
            create_source_resp = self._service.create_source(create_source_req, deadline_ms=DEFAULT_DEADLINE)
            assert create_source_resp.status == service_pb2.CreateSourceResponse.OK
            source_id = create_source_resp.source_id

            log_info('Inserting %d records into newly created source (id=%s)' %
                     (len(SaltfishTester.records), source_id))
            request = make_put_records_req(source_id, records=SaltfishTester.records)
            log_info("reals=%s; cats=%s" % (str(request.records[0].reals), str(request.records[0].cats)))
            response = self._service.put_records(request, deadline_ms=DEFAULT_DEADLINE)
            assert response.status == service_pb2.PutRecordsResponse.OK
            assert len(response.record_ids) == len(SaltfishTester.records)
            bucket = SOURCES_DATA_BUCKET_ROOT + source_id + '/'
            for record_id in response.record_ids:
                     log_info('Checking record at b=%s / k=%s)' % (bucket, record_id))
                     remote_data = self._riakc.bucket(bucket).get(record_id)
                     assert remote_data.encoded_data is not None
            log_info('Got message: "%s"' % response.msg)
        except rpcz.rpc.RpcDeadlineExceeded:
            log_error(FAILED_PREFIX + 'Deadline exceeded! The service did not respond in time')
            sys.exit(1)

    def test_put_records_with_nonexistent_source(self):
        try:
            source_id = "random_source_id_that_does_not_exist"
            log_info('Inserting %d records into a non-exsistent source (id=%s) Error expected' %
                     (len(SaltfishTester.records), source_id))
            request = make_put_records_req(source_id, records=SaltfishTester.records)
            response = self._service.put_records(request, deadline_ms=DEFAULT_DEADLINE)
            assert response.status == service_pb2.PutRecordsResponse.ERROR
            assert len(response.record_ids) == 0
            log_info('Got error message: "%s"' % response.msg)
        except rpcz.rpc.RpcDeadlineExceeded:
            log_error(FAILED_PREFIX + 'Deadline exceeded! The service did not respond in time')
            sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Muses server")
    parser.add_argument('--connect', type=str, default=DEFAULT_CONNECT, action='store',
                        help="ZMQ-style connect string where to connect to", metavar='STR')
    parser.add_argument('--riak-host', type=str, default=DEFAULT_RIAK_HOST, action='store',
                        help="Riak's hostname", metavar='HOST')
    parser.add_argument('--riak-port', type=int, default=DEFAULT_RIAK_PORT, action='store',
                        help="Riak's port (Protobuf protocol)", metavar='PORT')

    args = parser.parse_args()
    tester = SaltfishTester(args.connect, args.riak_host, args.riak_port)
    results = tester.run_tests()
    log_info(str(results))
