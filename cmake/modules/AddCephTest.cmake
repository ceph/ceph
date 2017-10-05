#AddCephTest is a module for adding tests to the "make check" target which runs CTest

#adds makes target/script into a test, test to check target, sets necessary environment variables
function(add_ceph_test test_name test_path)
  add_test(NAME ${test_name} COMMAND ${test_path} ${ARGN})
  add_dependencies(tests ${test_name})
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
    PROPERTY TIMEOUT 3600)
endfunction()

#sets uniform compiler flags and link libraries
function(add_ceph_unittest unittest_name)
  add_ceph_test(${unittest_name} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${unittest_name})
  target_link_libraries(${unittest_name} ${UNITTEST_LIBS})
  set_target_properties(${unittest_name} PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
endfunction()

