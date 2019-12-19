distutils_add_cython_module(cython_rbd
  rbd
  ${CMAKE_CURRENT_SOURCE_DIR}/rbd.pyx)
add_dependencies(cython_rbd librbd)
distutils_install_cython_module(cython_rbd)
