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

include_directories(${PROJECT_BINARY_DIR}/src)


add_library(source_pb_lib ${SOURCE_PB})
add_library(service_pb_lib ${SERVICE_PB})
target_link_libraries(service_pb_lib source_pb_lib)


add_executable(
  saltfish
    main.cpp
)

target_link_libraries(
  saltfish
    source_pb_lib
    service_pb_lib
    rpcz
    ${PROTOBUF_LIBRARY}
    ${Boost_LIBRARIES}
)

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
