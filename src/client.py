#!/usr/bin/env python

import rpcz
import source_pb2
import service_pb2
import service_rpcz


app = rpcz.Application()
stub = service_rpcz.SourceManager_Stub(app.create_rpc_channel("tcp://127.0.0.1:5555"))

row = source_pb2.SourceRow()
f = row.features.add()
f.real = 3.14159 * 2

resp = stub.put_row(row, deadline_ms=1000)
print('Received response, status=%d' % resp.status)
