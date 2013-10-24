#!/usr/bin/env python

import rpcz
import source_pb2
import service_pb2
import service_rpcz


app = rpcz.Application()
stub = service_rpcz.SourceManager_Stub(app.create_rpc_channel("tcp://127.0.0.1:5555"))

req = service_pb2.Request()
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
    resp = stub.push_rows(req, deadline_ms=1000)
    print('Received response, status=%d' % resp.status)
