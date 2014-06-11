#!/bin/bash
set -v
docker stop -t 0 saltfish_test && docker rm saltfish_test

docker stop -t 0 mariadbsql && docker rm mariadbsql
docker stop -t 0 riak && docker rm riak
docker stop -t 0 redis && docker rm redis
