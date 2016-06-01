add_custom_target(cython_rados
  COMMAND
  env
  CC=${PY_CC}
  CXX=${PY_CXX}
  LDSHARED=${PY_LDSHARED}
  OPT=\"-DNDEBUG -g -fwrapv -O2 -Wall\"
  LDFLAGS=-L${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
  CYTHON_BUILD_DIR=${CMAKE_BINARY_DIR}/src/pybind/rados
  CFLAGS=\"-iquote ${CMAKE_SOURCE_DIR}/src/include\"
  python ${CMAKE_SOURCE_DIR}/src/pybind/rados/setup.py build --build-base ${CYTHON_MODULE_DIR} --verbose 
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/rados
  DEPENDS rados)

