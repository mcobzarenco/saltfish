include(rpcz_functions)
find_package(ProtobufPlugin REQUIRED)

set(PROTOBUF_PREFIX "/usr/share/proto/reinferio")

protobuf_generate_cpp(
  CONFIG_PB_SRCS CONFIG_PB_HDRS proto/config.proto)
set(CONFIG_PB ${CONFIG_PB_SRCS} ${CONFIG_PB_HDRS})

protobuf_generate_cpp(
  SOURCE_PB_SRCS SOURCE_PB_HDRS ${PROTOBUF_PREFIX}/core.proto)
set(SOURCE_PB ${SOURCE_PB_SRCS} ${SOURCE_PB_HDRS})

protobuf_generate_cpp_f(
  SERVICE_PB_SRCS SERVICE_PB_HDRS "-I${PROTOBUF_PREFIX}" ${PROTOBUF_PREFIX}/saltfish.proto)
protobuf_generate_rpcz_f(
  SERVICE_RPCZ_SRCS SERVICE_RPCZ_HDRS "-I${PROTOBUF_PREFIX}" ${PROTOBUF_PREFIX}/saltfish.proto)
set(SERVICE_PB ${SERVICE_PB_SRCS} ${SERVICE_PB_HDRS}
  ${SERVICE_RPCZ_SRCS} ${SERVICE_RPCZ_HDRS})

protobuf_generate_python(SOURCE_PB_PY ${PROTOBUF_PREFIX}/core.proto)
protobuf_generate_python(SERVICE_PB_PY ${PROTOBUF_PREFIX}/saltfish.proto)
protobuf_generate_python_rpcz(SERVICE_RPCZ_PY ${PROTOBUF_PREFIX}/saltfish.proto)
# add_dependencies(${PROJECT_NAME} SOURCE_PB_PY SERVICE_PB_PY SERVICE_RPCZ_PY)

message(${PROTOBUF_INCLUDE_DIR})

add_library(source_pb_lib ${SOURCE_PB})
add_library(service_pb_lib ${SERVICE_PB})
target_link_libraries(service_pb_lib source_pb_lib)

add_library(
  sql_pool
    sql_pool.hpp
    sql_pool.cpp
)
target_link_libraries(
  sql_pool
    mysqlclient
    mysqlcppconn
    zmq
)

add_library(
  saltfish_service
    service.hpp
    service.cpp
    service_utils.hpp
)
target_link_libraries(
  saltfish_service
    sql_pool
    source_pb_lib
    service_pb_lib
    riakpp
    rpcz
    ${Boost_LIBRARIES}
    ${GLOG_LIBRARY}
    ${PROTOBUF_LIBRARY}
)

add_library(
  saltfish_server
    server.hpp
    server.cpp
    service.hpp
    publishers.hpp
    publishers.cpp
    ${CONFIG_PB}
)
target_link_libraries(
  saltfish_server
    sql_pool
    saltfish_service
    riakpp
    rpcz
    hiredis
    ${Boost_LIBRARIES}
    ${GLOG_LIBRARY}
    ${PROTOBUF_LIBRARY}
)


add_executable(
  saltfish
    main.cpp
    server.hpp
    saltfish_config.hpp
    ${CONFIG_PB}
)
target_link_libraries(
  saltfish
    saltfish_server
    ${Boost_LIBRARIES}
    ${GLOG_LIBRARY}
    ${PROTOBUF_LIBRARY}
)

add_executable(
  test_riak
    test_riak.cpp
)
target_link_libraries(
  test_riak
    riakpp
    ${Boost_LIBRARIES}
    ${PROTOBUF_LIBRARY}
)


# Install targets
# ==============================================================================
install(TARGETS saltfish DESTINATION bin)