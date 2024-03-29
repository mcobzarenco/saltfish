#!/bin/bash
set -e -v
SALTFISH_ROOT=$(cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd)

# Set up a MariaDB container:
MARIADB_USER=super
MARIADB_PASS=$(pwgen -s -1 16)
MARIADB_HOST_PORT=3306
docker run -d --name=mariadbsql -p ${MARIADB_HOST_PORT}:3306 \
    -e USER=${MARIADB_USER} -e PASS=${MARIADB_PASS} registry.reinfer.io/mariadb:latest

MARIADB_SCHEMA_DIR=/tmp/mariadb-reinferio
if [ -d $MARIADB_SCHEMA_DIR ]; then
    echo "[mariadb] $MARIADB_SCHEMA_DIR already exists" \
        " - running git pull as $SUDO_USER"
    cd $MARIADB_SCHEMA_DIR && sudo -u $SUDO_USER git pull
else
    echo "[mariadb] $MARIADB_SCHEMA_DIR does not exists" \
        " - running git clone as $SUDO_USER"
    sudo -u $SUDO_USER git clone git@github.com:reinferio/mariadb-reinferio.git \
        $MARIADB_SCHEMA_DIR
fi
sleep 3
cd $MARIADB_SCHEMA_DIR/src && mysql --host=127.0.0.1 --port=${MARIADB_HOST_PORT} \
    --user=${MARIADB_USER} --password=${MARIADB_PASS} -e "SOURCE all.sql;"

# Set up a Riak container:
docker run -d --name=riak -p 10017:8087 registry.reinfer.io/riak:latest

# Set up a Redis container:
docker run -d --name=redis -p 6379:6379 registry.reinfer.io/redis:latest

DOCKER_LINKS="--link=riak:riak --link=mariadbsql:mariadb --link=redis:redis"

echo "[saltfish] preparing to run integration tests.." && sleep 3
docker run -i -t -v ${SALTFISH_ROOT}:/src/saltfish ${DOCKER_LINKS} \
    --name=saltfish_test registry.reinfer.io/saltfish:latest \
    bash /src/saltfish/test/run_int_tests_container.sh
