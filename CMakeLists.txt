project(saltfish)
cmake_minimum_required (VERSION 2.8)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake_modules)
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -std=c++11 -Wall")


# Package Dependencies
# ==============================================================================
find_package(
  Boost 1.49
    COMPONENTS
      program_options
      system
      thread
    REQUIRED
)

find_package(ProtobufPlugin REQUIRED)
find_package(Glog REQUIRED)

include_directories(
  ${Boost_INCLUDE_DIR}
  ${GLOG_INCLUDE_DIR}
  ${PROTOBUF_INCLUDE_DIR}
)


add_subdirectory(src)



# add_library(rvm-pb rvm.pb.h rvm.pb.cc)
# add_library(rvm-rpcz rvm.rpcz.h rvm.rpcz.cc)
# target_link_libraries(rvm-rpcz rvm-pb)

# add_executable(
#   rvm
#     main.cpp
# )

# target_link_libraries(
#   rvm
#     rvm-pb
#     rvm-rpcz
#     rpcz
#     "/usr/local/lib/libprotobuf.so.8.0.0"
#     ${Boost_LIBRARIES}
#     ${GLOG_LIBRARY}
# )


# add_executable(
#   rvm-client
#     rvm-client.cpp
# )

# target_link_libraries(
#   rvm-client
#     rvm-pb
#     rvm-rpcz
#     rpcz
#     "/usr/local/lib/libprotobuf.so.8.0.0"
#     ${Boost_LIBRARIES}
#     ${GLOG_LIBRARY}
# )
