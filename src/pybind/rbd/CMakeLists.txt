add_custom_target(cython_rbd
  COMMAND
  env
  CC=${PY_CC}
  CXX=${PY_CXX}
  LDSHARED=${PY_LDSHARED}
  LDFLAGS=-L${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
  CYTHON_BUILD_DIR=${CMAKE_BINARY_DIR}/src/pybind/rbd
  CFLAGS=\"-iquote ${CMAKE_SOURCE_DIR}/src/include\"
  python ${CMAKE_SOURCE_DIR}/src/pybind/rbd/setup.py build --build-base ${CYTHON_MODULE_DIR} --verbose
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/rbd
  DEPENDS rbd)

