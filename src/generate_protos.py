#!/usr/bin/env python

from rpcz import compiler

compiler.generate_proto('proto/source.proto', '.')

compiler.generate_proto('proto/service.proto', '.')
compiler.generate_proto('proto/service.proto', '.', with_plugin='python_rpcz', suffix='_rpcz.py')
