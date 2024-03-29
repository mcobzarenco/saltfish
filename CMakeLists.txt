project(saltfish)
cmake_minimum_required (VERSION 2.8)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -std=c++11 -O3 -pedantic -Wall -g")

# Package Dependencies
# ==============================================================================
find_package(
  Boost 1.53
    COMPONENTS
      program_options
      system
      thread
    REQUIRED
)

find_package(ProtobufPlugin REQUIRED)
find_package(Glog REQUIRED)

find_package(CoreProto  REQUIRED)
find_package(SaltfishProto  REQUIRED)

include_directories(
  ${Boost_INCLUDE_DIR}
  ${GLOG_INCLUDE_DIR}
  ${PROTOBUF_INCLUDE_DIR}
)

include_directories(${PROJECT_BINARY_DIR}/src)

add_subdirectory(src)
add_subdirectory(test)
