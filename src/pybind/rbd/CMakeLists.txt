distutils_add_cython_module(cython${PYTHON_VERSION}_rbd
  rbd
  ${CMAKE_CURRENT_SOURCE_DIR}/rbd.pyx
  ${PYTHON_VERSION})
add_dependencies(cython${PYTHON_VERSION}_rbd librbd)
distutils_install_cython_module(cython${PYTHON_VERSION}_rbd
  ${PYTHON_VERSION})
