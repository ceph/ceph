distutils_add_cython_module(cython${PYTHON_VERSION}_rgw
  rgw
  ${CMAKE_CURRENT_SOURCE_DIR}/rgw.pyx
  ${PYTHON_VERSION})
add_dependencies(cython${PYTHON_VERSION}_rgw rgw)
distutils_install_cython_module(cython${PYTHON_VERSION}_rgw
  ${PYTHON_VERSION})
