#!/bin/bash
cd /src/saltfish/scripts
./config_from_env > env.conf
cat env.conf
pip install pymysql riak

saltfish --conf env.conf&
/src/saltfish/test/test_saltfish.py --conf env.conf
