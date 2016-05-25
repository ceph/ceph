#AddCephTest is a module for adding tests to the "make check" target which runs CTest

#adds makes target/script into a test, test to check target, sets necessary environment variables
function(add_ceph_test test_name test_path)
  add_test(NAME ${test_name} COMMAND ${test_path})
  add_dependencies(check ${test_name})
  set_property(TEST 
    ${test_name}
    PROPERTY ENVIRONMENT 
    CEPH_ROOT=${CMAKE_SOURCE_DIR}
    CEPH_BIN=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
    CEPH_LIB=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
    CEPH_BUILD_DIR=${CMAKE_BINARY_DIR}
    LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/lib
    PATH=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}:${CMAKE_SOURCE_DIR}/src:$ENV{PATH}
    PYTHONPATH=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cython_modules/lib.linux-x86_64-2.7:${CMAKE_SOURCE_DIR}/src/pybind
    CEPH_BUILD_VIRTUALENV=${CEPH_BUILD_VIRTUALENV})
endfunction()

#sets uniform compiler flags and link libraries
function(add_ceph_unittest unittest_name unittest_path)
  add_ceph_test(${unittest_name} ${unittest_path})
  target_link_libraries(${unittest_name} ${UNITTEST_LIBS})
  set_target_properties(${unittest_name} PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
endfunction()

