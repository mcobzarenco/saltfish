#!/usr/bin/env python
import sys
import argparse
import multiprocessing
from os.path import join
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
SOURCES_DATA_BUCKET = '/ml/sources/data/'

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
    req = service_pb2.CreateSourceRequest()
    if source_id:
        req.source_id = source_id
    for f in features:
        new_feat = req.schema.features.add()
        new_feat.name = f['name']
        new_feat.feature_type = f['type']
    return req


class SaltfishTester(object):
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
            log_info('Running %s' % t)
            results[t] = getattr(self, t)()
        log_success('SUCCESS | All %d test functions passed' % n_tests)
        return results

    def do_test_create_source(self, source_id=None):
        features = [
            {'name': 'timestamp_ms', 'type': source_pb2.Feature.REAL},
            {'name': 'return_stock1', 'type': source_pb2.Feature.REAL},
            {'name': 'logvol_stock1', 'type': source_pb2.Feature.REAL},
            {'name': 'sector_stock1', 'type': source_pb2.Feature.CATEGORICAL}
        ]
        if source_id:
            log_info('Creating a new source without setting the id..')
        else:
            log_info('Creating a new source with a given id (id=%s)..' % source_id)

        try:
            req = make_create_source_req(source_id, features)
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
            assert remote_features == features
            log_success(TEST_PASSED)

            log_info('Sending a 2nd identical request to check idempotentcy (source_id=%s)'
                     % source_id)
            response = self._service.create_source(req, deadline_ms=DEFAULT_DEADLINE)
            assert response.status == service_pb2.CreateSourceResponse.OK
            log_success(TEST_PASSED)

            log_info('Trying same id, but different schema. ' \
                     'Expecting error from the service (source_id=%s)' % source_id)
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

    def test_create_source_with_given_id(self):
        source_id = 'test_' + str(randint(0, 10000000))
        self.do_test_create_source(source_id)

    def test_create_source_with_no_id(self):
        self.do_test_create_source()

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
        log_info('Trying to generate 1000000 ids in one call. Expecting error from the service..')
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

    def test_push_rows(self):
        return
        req = service_pb2.PushRowsRequest()
        req.source_id = "abcdef"

        row = req.rows.add()
        row.reals.append(3.14159 * 2)
        row.reals.append(-1.0)
        row.categoricals.append("expensive")

        row = req.rows.add()
        row.reals.append(2.5)
        row.reals.append(-10.4)
        row.categoricals.append("cheap")

        for i in range(10):
            resp = self._service.push_rows(req, deadline_ms=1000)
            print('Received response, status=%d' % resp.status)


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
