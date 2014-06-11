#!/bin/bash
cd /src/saltfish/scripts
./config_from_env > env.conf
cat env.conf
pip install pymysql riak
pip install git+ssh://git@github.com/reinferio/py-reinferio.proto.git
pip install git+ssh://git@github.com/reinferio/core-proto.git
pip install git+ssh://git@github.com/reinferio/saltfish-proto.git

saltfish --conf env.conf&
/src/saltfish/test/test_saltfish.py --conf env.conf
