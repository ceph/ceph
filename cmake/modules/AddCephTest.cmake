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
  set_property(TEST ${test_name}
    PROPERTY TIMEOUT ${CEPH_TEST_TIMEOUT})
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
