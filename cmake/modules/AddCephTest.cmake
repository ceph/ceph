#AddCephTest is a module for adding tests to the "make check" target which runs CTest

#adds makes target/script into a test, test to check target, sets necessary environment variables
function(add_ceph_test test_name test_path)
  add_test(NAME ${test_name} COMMAND ${test_path} ${ARGN})
  if(TARGET ${test_name})
    add_dependencies(tests ${test_name})
  endif()
  set_property(TEST
    ${test_name}
    PROPERTY ENVIRONMENT 
    CEPH_ROOT=${CMAKE_SOURCE_DIR}
    CEPH_BIN=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
    CEPH_LIB=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
    CEPH_BUILD_DIR=${CMAKE_BINARY_DIR}
    LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/lib
    PATH=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}:${CMAKE_SOURCE_DIR}/src:$ENV{PATH}
    PYTHONPATH=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cython_modules/lib.${PYTHON${PYTHON_VERSION}_VERSION_MAJOR}:${CMAKE_SOURCE_DIR}/src/pybind
    CEPH_BUILD_VIRTUALENV=${CEPH_BUILD_VIRTUALENV})
  # none of the tests should take more than 1 hour to complete
  set_property(TEST
    ${test_name}
    PROPERTY TIMEOUT ${CEPH_TEST_TIMEOUT})
endfunction()

option(WITH_GTEST_PARALLEL "Enable running gtest based tests in parallel" OFF)
if(WITH_GTEST_PARALLEL)
  set(gtest_parallel_source_dir ${CMAKE_CURRENT_BINARY_DIR}/gtest-parallel)
  include(ExternalProject)
  ExternalProject_Add(gtest-parallel_ext
    SOURCE_DIR "${gtest_parallel_source_dir}"
    GIT_REPOSITORY "https://github.com/google/gtest-parallel.git"
    GIT_TAG "master"
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND "")
  add_dependencies(tests gtest-parallel_ext)
  find_package(PythonInterp REQUIRED)
  set(GTEST_PARALLEL_COMMAND
    ${PYTHON_EXECUTABLE} ${gtest_parallel_source_dir}/gtest-parallel)
endif()

#sets uniform compiler flags and link libraries
function(add_ceph_unittest unittest_name)
  set(UNITTEST "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${unittest_name}")
  # If the second argument is "parallel", it means we want a parallel run
  if(WITH_GTEST_PARALLEL AND "${ARGV1}" STREQUAL "parallel")
    set(UNITTEST ${GTEST_PARALLEL_COMMAND} ${UNITTEST})
  endif()
  add_ceph_test(${unittest_name} "${UNITTEST}")
  target_link_libraries(${unittest_name} ${UNITTEST_LIBS})
endfunction()
