distutils_add_cython_module(cython_cephfs
  cephfs
  ${CMAKE_CURRENT_SOURCE_DIR}/cephfs.pyx)
add_dependencies(cython_cephfs cephfs)
distutils_install_cython_module(cython_cephfs)
