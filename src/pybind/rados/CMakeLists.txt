add_custom_target(cython_rados
  COMMAND
  LDFLAGS=-L${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
  CYTHON_BUILD_DIR=${CMAKE_BINARY_DIR}/src/pybind/rados
  CFLAGS=\"-I${CMAKE_SOURCE_DIR}/src/include -std=c++11\"
  python ${CMAKE_SOURCE_DIR}/src/pybind/rados/setup.py build --build-base ${CYTHON_MODULE_DIR} --verbose 
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/rados
  DEPENDS rados)

