#AddCephTest is a module for adding tests to the "make check" target which runs CTest

#adds makes target/script into a test, test to check target, sets necessary environment variables
function(add_ceph_test test_name test_path)
  add_test(NAME ${test_name} COMMAND ${test_path} ${ARGN}
    COMMAND_EXPAND_LISTS)
  if(TARGET ${test_name})
    add_dependencies(tests ${test_name})
    set_property(TARGET ${test_name}
      PROPERTY EXCLUDE_FROM_ALL TRUE)
  endif()
  set_property(TEST ${test_name}
    PROPERTY ENVIRONMENT 
    CEPH_ROOT=${CMAKE_SOURCE_DIR}
    CEPH_BIN=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
    CEPH_LIB=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
    CEPH_BUILD_DIR=${CMAKE_BINARY_DIR}
    LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/lib
    PATH=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}:${CMAKE_SOURCE_DIR}/src:$ENV{PATH}
    PYTHONPATH=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cython_modules/lib.3:${CMAKE_SOURCE_DIR}/src/pybind
    CEPH_BUILD_VIRTUALENV=${CEPH_BUILD_VIRTUALENV})
  if(WITH_UBSAN)
    set_property(TEST ${test_name}
      APPEND
      PROPERTY ENVIRONMENT
      UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1)
  endif()
  if(WITH_ASAN)
    # AddressSanitizer: odr-violation: global 'ceph::buffer::list::always_empty_bptr' at
    # /home/jenkins-build/build/workspace/ceph-pull-requests/src/common/buffer.cc:1267:34
    # see https://tracker.ceph.com/issues/65098
    set_property(TEST ${test_name}
      APPEND
      PROPERTY ENVIRONMENT
      ASAN_OPTIONS=detect_odr_violation=0)
  endif()
  set_property(TEST ${test_name}
    PROPERTY TIMEOUT ${CEPH_TEST_TIMEOUT})
  # Crimson seastar unittest always run with --smp N to start N threads. By default, crimson seastar unittest
  # will take cpu cores[0, N), starting one thread per core. When running many crimson seastar unittests
  # parallely, the front N cpu cores are shared, and the left cpu cores are idle. Lots of cpu cores are wasted.
  # Using CTest resource allocation feature(https://cmake.org/cmake/help/latest/manual/ctest.1.html#resource-allocation),
  # ctest can specify cpu cores resources to crimson seastar unittests.
  # 3 steps to enable CTest resource allocation feature:
  #  Step 1: Generate a resource specification file to describe available resource, $(nproc) CPUs with id 0 to $(nproc) - 1
  #  Step 2: Set RESOURCE_GROUPS property to a test with value "${smp_count},cpus:1"
  #  Step 3: Read a series of environment variables CTEST_RESOURCE_GROUP_* and set seastar smp_opts while running a test
  list(FIND ARGV "--smp" smp_pos)
  if(smp_pos GREATER -1)
    if(smp_pos EQUAL ARGC)
      message(FATAL_ERROR "${test_name} --smp requires an argument")
    endif()
    math(EXPR i "${smp_pos} + 1")
    list(GET ARGV ${i} smp_count)
    set_property(TEST ${test_name}
      PROPERTY RESOURCE_GROUPS "${smp_count},cpus:1")
  endif()
endfunction()

option(WITH_GTEST_PARALLEL "Enable running gtest based tests in parallel" OFF)
if(WITH_GTEST_PARALLEL)
  if(NOT TARGET gtest-parallel_ext)
    set(gtest_parallel_source_dir ${CMAKE_CURRENT_BINARY_DIR}/gtest-parallel)
    include(ExternalProject)
    ExternalProject_Add(gtest-parallel_ext
      SOURCE_DIR "${gtest_parallel_source_dir}"
      GIT_REPOSITORY "https://github.com/google/gtest-parallel.git"
      GIT_TAG "master"
      GIT_SHALLOW TRUE
      CONFIGURE_COMMAND ""
      BUILD_COMMAND ""
      INSTALL_COMMAND "")
    add_dependencies(tests gtest-parallel_ext)
    set(GTEST_PARALLEL_COMMAND
      ${Python3_EXECUTABLE} ${gtest_parallel_source_dir}/gtest-parallel)
  endif()
endif()

#sets uniform compiler flags and link libraries
function(add_ceph_unittest unittest_name)
  set(UNITTEST "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${unittest_name}")
  cmake_parse_arguments(UT "PARALLEL" "" "" ${ARGN})
  if(WITH_GTEST_PARALLEL AND UT_PARALLEL)
    set(UNITTEST ${GTEST_PARALLEL_COMMAND} ${UNITTEST})
  endif()
  add_ceph_test(${unittest_name} "${UNITTEST}" ${UT_UNPARSED_ARGUMENTS})
  target_link_libraries(${unittest_name} ${UNITTEST_LIBS})
endfunction()

function(add_tox_test name)
  set(test_name run-tox-${name})
  set(venv_path ${CEPH_BUILD_VIRTUALENV}/${name}-virtualenv)
  cmake_parse_arguments(TOXTEST "" "TOX_PATH" "TOX_ENVS" ${ARGN})
  if(DEFINED TOXTEST_TOX_PATH)
    set(tox_path ${TOXTEST_TOX_PATH})
  else()
    set(tox_path ${CMAKE_CURRENT_SOURCE_DIR})
  endif()
  if(DEFINED TOXTEST_TOX_ENVS)
    list(APPEND tox_envs ${TOXTEST_TOX_ENVS})
  else()
    list(APPEND tox_envs py3)
  endif()
  string(REPLACE ";" "," tox_envs "${tox_envs}")
  add_test(
    NAME setup-venv-for-${name}
    COMMAND ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh --python=${Python3_EXECUTABLE} ${venv_path}
    WORKING_DIRECTORY ${tox_path})
  set_tests_properties(setup-venv-for-${name} PROPERTIES
    FIXTURES_SETUP venv-for-${name})
  add_test(
    NAME teardown-venv-for-${name}
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${venv_path})
  set_tests_properties(teardown-venv-for-${name} PROPERTIES
    FIXTURES_CLEANUP venv-for-${name})
  add_test(
    NAME ${test_name}
    COMMAND ${CMAKE_SOURCE_DIR}/src/script/run_tox.sh
              --source-dir ${CMAKE_SOURCE_DIR}
              --build-dir ${CMAKE_BINARY_DIR}
              --tox-path ${tox_path}
              --tox-envs ${tox_envs}
              --venv-path ${venv_path})
  set_tests_properties(${test_name} PROPERTIES
    FIXTURES_REQUIRED venv-for-${name})
  set_property(
    TEST ${test_name}
    PROPERTY ENVIRONMENT
    CEPH_ROOT=${CMAKE_SOURCE_DIR}
    CEPH_BIN=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
    CEPH_LIB=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
    CEPH_BUILD_VIRTUALENV=${CEPH_BUILD_VIRTUALENV}
    LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/lib
    PATH=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}:${CMAKE_SOURCE_DIR}/src:${CMAKE_CURRENT_BINARY_DIR}:$ENV{PATH}
    PYTHONPATH=${CMAKE_SOURCE_DIR}/src/pybind)
  list(APPEND tox_test run-tox-${name})
endfunction()
