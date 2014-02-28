#!/bin/bash
SALTFISH_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"

MARIADB_USER="super"
MARIADB_PASS="$(pwgen -s -1 16)"
MARIADB_HOST_PORT=3306

docker run -d -name mariadbsql -p ${MARIADB_HOST_PORT}:3306 \
    -e USER=${MARIADB_USER} -e PASS=${MARIADB_PASS} registry.reinfer.io/mariadb
git clone git@github.com:reinferio/mariadb-reinferio.git /tmp/mariadb-reinferio
cd /tmp/mariadb-reinferio/src
mysql -h 127.0.0.1 --user=${MARIADB_USER} --password=${MARIADB_PASS} < all.sql

docker run -d -name riak registry.reinfer.io/riak
docker run -d -name rabbitmq rabbitmq

DOCKER_LINKS="--link=riak:riak --link=mariadbsql:mariadb --link=rabbitmq:rabbitmq"
docker run -i -t -v ${SALTFISH_ROOT}:/src/saltfish ${DOCKER_LINKS} \
    -name saltfish_test saltfish bash /src/saltfish/test/run_int_tests_container.sh

docker stop -t 0 saltfish_test && docker rm saltfish_test

docker stop -t 0 mariadbsql && docker rm mariadbsql
docker stop -t 0 riak && docker rm riak
docker stop -t 0 rabbitmq && docker rm rabbitmq
