# set(Boost_USE_STATIC_LIBS ON)
# find_package(Boost COMPONENTS system thread)

# include_directories(${PROTOBUF_INCLUDE_DIRS})
# include_directories(${Boost_INCLUDE_DIRS})
include(rpcz_functions)
find_package(ProtobufPlugin REQUIRED)

protobuf_generate_cpp(SOURCE_PB_SRCS SOURCE_PB_HDRS proto/source.proto)
set(SOURCE_PB ${SOURCE_PB_SRCS} ${SOURCE_PB_HDRS})

protobuf_generate_cpp(SERVICE_PB_SRCS SERVICE_PB_HDRS proto/service.proto)
protobuf_generate_rpcz(SERVICE_RPCZ_SRCS SERVICE_RPCZ_HDRS proto/service.proto)
set(SERVICE_PB ${SERVICE_PB_SRCS} ${SERVICE_PB_HDRS} ${SERVICE_RPCZ_SRCS} ${SERVICE_RPCZ_HDRS})

protobuf_generate_python(SOURCE_PB_PY proto/source.proto)
protobuf_generate_python(SERVICE_PB_PY proto/service.proto)
protobuf_generate_python_rpcz(SERVICE_RPCZ_PY proto/service.proto)
# add_dependencies(${PROJECT_NAME} SOURCE_PB_PY SERVICE_PB_PY SERVICE_RPCZ_PY)

include_directories(${PROJECT_BINARY_DIR}/src)


add_library(source_pb_lib ${SOURCE_PB})
add_library(service_pb_lib ${SERVICE_PB})
target_link_libraries(service_pb_lib source_pb_lib)


add_library(
  riak_proxy
    riak_proxy.hpp
    riak_proxy.cpp
)
target_link_libraries(
  riak_proxy
    riak-cpp
    ${Boost_LIBRARIES}
    ${GLOG_LIBRARY}
    ${PROTOBUF_LIBRARY}
)


add_library(
  saltfish_service
    service.hpp
    service.cpp
    riak_proxy.hpp
    ${SERVICE_PB}
)
target_link_libraries(
  saltfish_service
    riak_proxy
    source_pb_lib
    service_pb_lib
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
    riak_proxy.hpp
    ${SERVICE_PB}
)
target_link_libraries(
  saltfish_server
    riak_proxy
    saltfish_service
    ${Boost_LIBRARIES}
    ${GLOG_LIBRARY}
    ${PROTOBUF_LIBRARY}
)


add_executable(
  saltfish
    main.cpp
    server.hpp
)
target_link_libraries(
  saltfish
    saltfish_server
    ${Boost_LIBRARIES}
    ${GLOG_LIBRARY}
    ${PROTOBUF_LIBRARY}
)

add_dependencies(${PROJECT_NAME} SOURCE_PB_PY SERVICE_PB_PY SERVICE_RPCZ_PY)


add_executable(
  main2
    main2.cpp
)

target_link_libraries(
  main2
    riak-cpp
    pthread
    ${PROTOBUF_LIBRARY}
    ${Boost_LIBRARIES}
)