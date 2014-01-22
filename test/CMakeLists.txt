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
  tests
    test_service_utils.cpp
    ../src/service_utils.hpp
    ${source_dir}
)
add_dependencies(tests GTest)
add_dependencies(tests GMock)
target_link_libraries(
  tests
    gtest
    saltfish_service
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
