#!/usr/bin/env python

import argparse
import logging
import multiprocessing
log = multiprocessing.log_to_stderr()
log.setLevel(logging.INFO)

import rpcz
import riak

try:
    import source_pb2
    import service_pb2
    import service_rpcz
except ImportError:
    from os.path import join
    from rpcz import compiler
    PROTO_ROOT = join('..', 'src', 'proto')

    log.info('protobufs libs not found, generating them..')
    compiler.generate_proto(join(PROTO_ROOT, 'source.proto'), '.')
    compiler.generate_proto(join(PROTO_ROOT, 'service.proto'), '.')
    compiler.generate_proto(join(PROTO_ROOT, 'service.proto'), '.',
                            with_plugin='python_rpcz', suffix='_rpcz.py')
    import source_pb2
    import service_pb2
    import service_rpcz


DEFAULT_CONNECT = 'tcp://localhost:5555'
DEFAULT_RIAK_HOST = 'localhost'
DEFAULT_RIAK_PORT = 10017


class SaltfishTester(object):
    def __init__(self, connect_str, riak_host, riak_port):
        self._app = rpcz.Application()
        self._channel = self._app.create_rpc_channel(connect_str)
        self._service = service_rpcz.SourceManager_Stub(self._channel)
        self._riakc = riak.RiakClient(protocol='pbc', host=riak_host, pb_port=riak_port)

    def run_tests(self):
        results = {}
        for t in filter(lambda x: x.startswith('test'), dir(self)):
            log.info('Running %s' % t)
            results[t] = getattr(self, t)()
        return results

    def test_generate_id(self):
        N = 1000
        log.info('Generating %d ids in one call to the service..' % N)
        for n in xrange(N):
            resp = self._service.push_rows(req, deadline_ms=1000)
            returned_status = resp.
            assert
            print('Received response, status=%d' % resp.status)

        log.info('Calling the service %d times, generating 1 id per call' % N)


    def test_push_rows(self):
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
    log.info(results)
