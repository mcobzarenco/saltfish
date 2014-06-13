include(rpcz_functions)
find_package(ProtobufPlugin REQUIRED)

execute_process(COMMAND touch main.cpp
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

protobuf_generate_cpp(
  CONFIG_PB_SRCS CONFIG_PB_HDRS proto/config.proto)
set(CONFIG_PB ${CONFIG_PB_SRCS} ${CONFIG_PB_HDRS})

protobuf_generate_cpp_f(
  CORE_PB_SRCS CORE_PB_HDRS
  ${COMMON_PROTO_ROOT} ${COREPROTO_PATH})
set(CORE_PB ${CORE_PB_SRCS} ${CORE_PB_HDRS})

protobuf_generate_cpp_f(
  SERVICE_PB_SRCS SERVICE_PB_HDRS
  ${COMMON_PROTO_ROOT} ${SALTFISHPROTO_PATH})
protobuf_generate_rpcz_f(
  SERVICE_RPCZ_SRCS SERVICE_RPCZ_HDRS
  ${COMMON_PROTO_ROOT} ${SALTFISHPROTO_PATH})
set(SERVICE_PB ${SERVICE_PB_SRCS} ${SERVICE_PB_HDRS}
  ${SERVICE_RPCZ_SRCS} ${SERVICE_RPCZ_HDRS})

add_library(source_pb_lib ${CORE_PB})
add_library(service_pb_lib ${SERVICE_PB})
target_link_libraries(service_pb_lib source_pb_lib)

add_library(
  sql_pool
    sql.hpp
    sql.cpp
    sql_errors.hpp
    sql_errors.cpp
)
target_link_libraries(
  sql_pool
    mysqlclient
    mysqlcppconn
    zmq
    service_pb_lib
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