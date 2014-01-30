include(ExternalProject)

include_directories(../src)

ExternalProject_Add(
    GTest
    URL http://googletest.googlecode.com/files/gtest-1.7.0.zip
    TIMEOUT 30
    INSTALL_COMMAND ""
)
ExternalProject_Get_Property(GTest binary_dir)
ExternalProject_Get_Property(GTest source_dir)
include_directories(${source_dir}/include)
link_directories(${binary_dir})
enable_testing(true)

ExternalProject_Add(
    GMock
    URL http://googlemock.googlecode.com/files/gmock-1.7.0.zip
    TIMEOUT 30
    INSTALL_COMMAND ""
)
ExternalProject_Get_Property(GMock binary_dir)
ExternalProject_Get_Property(GMock source_dir)
include_directories(${source_dir}/include)
link_directories(${binary_dir})


add_executable(
  test_service_utils
    test_service_utils.cpp
    ../src/service_utils.hpp
    ${source_dir}
)
add_dependencies(test_service_utils GTest)
add_dependencies(test_service_utils GMock)
target_link_libraries(
  test_service_utils
    gtest
    saltfish_service
)

add_executable(
  test_tasklet
    test_tasklet.cpp
    ../src/tasklet.hpp
    ${source_dir}
)
add_dependencies(test_tasklet GTest)
add_dependencies(test_tasklet GMock)
target_link_libraries(
  test_tasklet
    gtest
    zmq
    pthread
    ${GLOG_LIBRARY}
)


# file(GLOB TEST_FILES *_test.cpp)

# add_executable(
#   unittests
#     unittests_main.cpp
#     ${TEST_FILES}
# )

# target_link_libraries(
#   unittests
#     riakpp gtest
# )

# add_custom_target(
#     test unittests 1>&2
#     DEPENDS unittests
# )
