#AddCephTest is a module for adding tests to the "make check" target which runs CTest

execute_process(
    COMMAND uname -m
    OUTPUT_VARIABLE ARCHSTR
    OUTPUT_STRIP_TRAILING_WHITESPACE)

if(LINUX)
  set(OSSTR linux)
elseif(FREEBSD)
  execute_process(
    COMMAND uname -r
    OUTPUT_VARIABLE OSVER
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  set(OSSTR freebsd-${OSVER})
else()
  message(FATAL_ERROR "unsupported OS, please fix PYTHONPATH construction first.")
  #
  # If you get this message for your OS you need to work out where
  # cython would put its libs.
  # For Linux is is something like:
  #    ceph/build/lib/cython_modules/lib.linux-x86_64-2.7
  # For FreeBSD it is something like:
  #    ceph/build/lib/cython_modules/lib.freebsd-11.0-CURRENT-amd64-2.7
endif(LINUX)

set(PYVER "${PYTHON_VERSION_MAJOR}.${PYTHON_VERSION_MINOR}")

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
    PYTHONPATH=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cython_modules/lib.${OSSTR}-${ARCHSTR}-${PYVER}:${CMAKE_SOURCE_DIR}/src/pybind
    CEPH_BUILD_VIRTUALENV=${CEPH_BUILD_VIRTUALENV})
  # none of the tests should take more than 1 hour to complete
  set_property(TEST
    ${test_name}
    PROPERTY TIMEOUT 3600)
endfunction()

#sets uniform compiler flags and link libraries
function(add_ceph_unittest unittest_name unittest_path)
  add_ceph_test(${unittest_name} ${unittest_path})
  target_link_libraries(${unittest_name} ${UNITTEST_LIBS})
  set_target_properties(${unittest_name} PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
endfunction()

