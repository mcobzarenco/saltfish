include(ExternalProject)

include_directories(../src)
include_directories(../src/treadmill)

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

add_executable(
  test_treadmill
    test_moments_summarizer.cpp
    test_categorical_histogram_summarizer.cpp
    test_record_summarizer.cpp
    ${source_dir}
)
target_link_libraries(
  test_treadmill
    treadmill
    record_summarizer
    pthread
    gtest
    gtest_main
)

# add_custom_target(
#     test test_treadmill --gtest_catch_exceptions=1 --gtest_color=yes 1>&2
#     DEPENDS test_treadmill
# )


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
