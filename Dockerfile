FROM ubuntu:14.04

MAINTAINER Marius Cobzarenco <marius@reinfer.io>


RUN apt-get update && \
    apt-get install -y build-essential cmake && \
    apt-get install -y clang-3.4 && \
    apt-get install -y protobuf-compiler libprotobuf-dev libprotoc-dev && \
    apt-get install -y libboost-dev libboost-system-dev && \
    apt-get install -y ssh git tar

ENV CC clang
ENV CXX clang++

# Set private key for cloning private repos
RUN mkdir -p /.ssh
ADD etc/id_rsa /.ssh/
RUN chmod 600 /.ssh/id_rsa
RUN echo "IdentityFile /.ssh/id_rsa" >> /etc/ssh/ssh_config
RUN echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

RUN mkdir -p /src
WORKDIR /src

# Install MySQL Connector C++
RUN apt-get install -y libmysqlclient-dev libmysqlcppconn-dev

# Install MySQL Connector C++ from source
# RUN apt-get install -y libmysqlclient-dev
# ADD http://dev.mysql.com/get/Downloads/Connector-C++/mysql-connector-c++-1.1.3.tar.gz /src/
# RUN cd /src && tar xvf mysql-connector-c++-1.1.3.tar.gz
# RUN cd /src/mysql-connector-c++-1.1.3 && mkdir build && cd build && cmake .. && make -j4
# RUN cd /src/mysql-connector-c++-1.1.3/build/cppconn && cp config.h ../../cppconn/ && make install
# RUN cd /src/mysql-connector-c++-1.1.3/build/driver && make install

# Install zmq
RUN apt-get install -y libzmq3-dev
RUN git clone https://github.com/zeromq/cppzmq.git
RUN cd cppzmq && cp zmq.hpp /usr/include/

# Install riakpp
RUN apt-get install -y libboost-program-options-dev
RUN git clone https://github.com/reinferio/riakpp.git
RUN cd riakpp && mkdir build && cd build && cmake .. && make && make install

# Install rpcz
RUN apt-get install -y python-dev python-pip
RUN pip install protobuf
RUN git clone https://github.com/reinferio/rpcz.git
RUN cd rpcz && mkdir build && cd build && cmake .. && make && make install
RUN cd rpcz/python && python setup.py build && python setup.py install

# Install hiredis (Redis C client)
RUN git clone https://github.com/redis/hiredis.git
RUN cd hiredis && make && make install

# Install core-proto
RUN git clone git@github.com:reinferio/core-proto.git
RUN cd core-proto && ./install.py

# Install saltfish-proto
RUN git clone git@github.com:reinferio/saltfish-proto.git
RUN cd saltfish-proto && ./install.py

# Install saltfish
RUN apt-get install -y libgoogle-glog-dev \
    libboost-thread-dev libboost-program-options-dev
RUN mkdir -p /src/saltfish
ADD . /src/saltfish/
WORKDIR /src/saltfish
RUN rm -Rf build && mkdir -p build
RUN cd build && cmake .. && make && make install
RUN build/test/test_service_utils
RUN build/test/test_tasklet


ENTRYPOINT ["saltfish"]
