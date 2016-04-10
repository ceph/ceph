add_custom_target(cython_cephfs
  COMMAND
  env
  CC=${PY_CC}
  CXX=${PY_CXX}
  LDSHARED=${PY_LDSHARED}
  LDFLAGS=-L${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
  CYTHON_BUILD_DIR=${CMAKE_BINARY_DIR}/src/pybind/cephfs
  CFLAGS=\"-I${CMAKE_SOURCE_DIR}/src -I${CMAKE_BINARY_DIR}/include -I${CMAKE_SOURCE_DIR}/src/include -std=c++11\"
  python ${CMAKE_SOURCE_DIR}/src/pybind/cephfs/setup.py build --build-base ${CYTHON_MODULE_DIR} --verbose 
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/cephfs
  DEPENDS rados cephfs)

