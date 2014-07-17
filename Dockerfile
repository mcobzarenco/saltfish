FROM ubuntu:13.10

MAINTAINER Marius Cobzarenco <marius@reinfer.io>


RUN apt-get update
RUN apt-get install -y wget

# Install clang-3.5
RUN wget -O - http://llvm.org/apt/llvm-snapshot.gpg.key|sudo apt-key add -
RUN echo "" >> /etc/apt/sources.list
RUN echo "deb http://llvm.org/apt/saucy/ llvm-toolchain-saucy main" >> /etc/apt/sources.list
RUN echo "deb-src http://llvm.org/apt/saucy/ llvm-toolchain-saucy main" >> /etc/apt/sources.list
RUN apt-get update
RUN apt-get install -y clang-3.4
ENV CC clang
ENV CXX clang++

RUN apt-get install -y build-essential cmake
RUN apt-get install -y protobuf-compiler libprotobuf-dev libprotoc-dev
RUN apt-get install -y libboost-dev libboost-system-dev
RUN apt-get install -y ssh git tar

RUN mkdir -p /src

# Install MySQL Connector C++
RUN apt-get install -y libmysqlclient-dev libmysqlcppconn-dev
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
RUN cd /src && git clone https://github.com/zeromq/cppzmq.git
RUN cd /src/cppzmq && cp zmq.hpp /usr/include/

# Install riakpp
RUN apt-get install -y libboost-program-options-dev
RUN cd /src && git clone https://github.com/reinferio/riakpp.git
RUN cd /src/riakpp && mkdir build && cd build && cmake .. && make -j4 && make install

# Install rpcz
RUN apt-get install -y python-dev python-pip
RUN pip install protobuf
RUN cd /src && git clone https://github.com/reinferio/rpcz.git
RUN cd /src/rpcz && mkdir build && cd build && cmake .. && make && make install
RUN cd /src/rpcz/python && python setup.py build && python setup.py install

# Install hiredis (Redis C client)
RUN cd /src && git clone https://github.com/redis/hiredis.git
RUN cd /src/hiredis && make && make install

ADD etc/id_rsa /src/ && chmod 600 /src/id_rsa
RUN echo "IdentityFile /src/id_rsa" >> /etc/ssh/ssh_config
RUN echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

RUN cd /src && git clone git@github.com:reinferio/core-proto.git
RUN cd /src/core-proto && ./install.sh

RUN cd /src && git clone git@github.com:reinferio/saltfish-proto.git
RUN cd /src/saltfish-proto && ./install.sh

# Install saltfish
RUN apt-get install -y libgoogle-glog-dev libboost-thread-dev libboost-program-options-dev
RUN mkdir -p /src/saltfish
ADD . /src/saltfish/
RUN cd /src/saltfish && rm -Rf build && mkdir -p build
RUN cd /src/saltfish/build && CXX=clang++ cmake .. && make -j4 && make install
RUN /src/saltfish/build/test/test_service_utils
RUN /src/saltfish/build/test/test_tasklet

#RUN cd /src/saltfish /src/saltfish/build/test/test_tasklet

#ENTRYPOINT saltfish
CMD saltfish
