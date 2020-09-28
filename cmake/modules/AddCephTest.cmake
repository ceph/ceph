#AddCephTest is a module for adding tests to the "make check" target which runs CTest

macro(set_test_props labels cost cost_default timeout)
  if("${timeout}" GREATER 0)
    set_property(TEST ${test_name} PROPERTY TIMEOUT ${timeout})
  else()
    set_property(TEST ${test_name} PROPERTY TIMEOUT ${CEPH_TEST_TIMEOUT})
  endif()
  if("${cost}" GREATER 0)
    set_property(TEST ${test_name} PROPERTY COST ${cost})
  else()
    set_property(TEST ${test_name} PROPERTY COST ${cost_default})
  endif()
  if(NOT "${labels}" EQUAL "")
    set_property(TEST ${test_name} APPEND PROPERTY LABELS "${labels}")
  endif()
endmacro()

#adds makes target/script into a test, test to check target, sets necessary environment variables
function(add_ceph_test test_name test_path)
  cmake_parse_arguments(PROPS "" "COST;TIMEOUT" "LABELS" ${ARGN})
  add_test(NAME ${test_name} COMMAND ${test_path} ${PROPS_UNPARSED_ARGUMENTS})
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
    PYTHONPATH=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cython_modules/lib.3:${CMAKE_SOURCE_DIR}/src/pybind
    CEPH_BUILD_VIRTUALENV=${CEPH_BUILD_VIRTUALENV})
    set_test_props("${PROPS_LABELS}" "${PROPS_COST}" 5.0 "${PROPS_TIMEOUT}")
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
      CONFIGURE_COMMAND ""
      BUILD_COMMAND ""
      INSTALL_COMMAND "")
    add_dependencies(tests gtest-parallel_ext)
    find_package(Python3 QUIET REQUIRED)
    set(GTEST_PARALLEL_COMMAND
      ${Python3_EXECUTABLE} ${gtest_parallel_source_dir}/gtest-parallel)
  endif()
endif()

#sets uniform compiler flags and link libraries
function(add_ceph_unittest test_name)
  cmake_parse_arguments(PROPS "" "COST;TIMEOUT" "LABELS" ${ARGN})
  set(UNITTEST "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${test_name}")
  # If the second argument is "parallel", it means we want a parallel run
  if(WITH_GTEST_PARALLEL AND "${ARGV1}" STREQUAL "parallel")
    set(UNITTEST ${GTEST_PARALLEL_COMMAND} ${UNITTEST})
  endif()
  add_ceph_test(${test_name} "${UNITTEST}" "unittest" LABELS "unittest")
  target_link_libraries(${test_name} ${UNITTEST_LIBS})
  set_test_props("${PROPS_LABELS}" "${PROPS_COST}" 1.0 "${PROPS_TIMEOUT}")
endfunction()

function(add_tox_test name)
  set(test_name run-tox-${name})
  set(venv_path ${CEPH_BUILD_VIRTUALENV}/${name}-virtualenv)
  cmake_parse_arguments(TOXTEST "" "TOX_PATH;COST;TIMEOUT" "TOX_ENVS;LABELS" ${ARGN})
  if(DEFINED TOXTEST_TOX_PATH)
    set(tox_path ${TOXTEST_TOX_PATH})
  else()
    set(tox_path ${CMAKE_CURRENT_SOURCE_DIR})
  endif()
  list(APPEND tox_envs py3)
  if(DEFINED TOXTEST_TOX_ENVS)
    list(APPEND tox_envs ${TOXTEST_TOX_ENVS})
  endif()
  string(REPLACE ";" "," tox_envs "${tox_envs}")
  find_package(Python3 QUIET REQUIRED)
  add_custom_command(
    OUTPUT ${venv_path}/bin/activate
    COMMAND ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh --python="${Python3_EXECUTABLE}" ${venv_path}
    WORKING_DIRECTORY ${tox_path}
    COMMENT "preparing venv for ${name}")
  add_custom_target(${name}-venv
    DEPENDS ${venv_path}/bin/activate)
  add_dependencies(tests ${name}-venv)
  add_test(
    NAME ${test_name}
    COMMAND ${CMAKE_SOURCE_DIR}/src/script/run_tox.sh
              --source-dir ${CMAKE_SOURCE_DIR}
              --build-dir ${CMAKE_BINARY_DIR}
              --tox-path ${tox_path}
              --tox-envs ${tox_envs}
              --venv-path ${venv_path})
  set_property(
    TEST ${test_name}
    PROPERTY ENVIRONMENT
    CEPH_ROOT=${CMAKE_SOURCE_DIR}
    CEPH_BIN=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
    CEPH_LIB=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
    CEPH_BUILD_VIRTUALENV=${CEPH_BUILD_VIRTUALENV}
    LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/lib
    PATH=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}:${CMAKE_SOURCE_DIR}/src:$ENV{PATH}
    PYTHONPATH=${CMAKE_SOURCE_DIR}/src/pybind)
  list(APPEND tox_test run-tox-${name})
  set_tests_properties(${test_name} PROPERTIES LABELS "tox")
  set_test_props("${TOXTEST_LABELS}" "${TOXTEST_COST}" 25.0 "${TOXTEST_TIMEOUT}")
endfunction()

