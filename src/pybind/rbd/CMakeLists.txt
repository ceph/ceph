distutils_add_cython_module(cython${PYTHON_VERSION}_rbd ${CMAKE_CURRENT_SOURCE_DIR}/rbd.pyx)
add_dependencies(cython${PYTHON_VERSION}_rbd rbd)
distutils_install_cython_module(cython${PYTHON_VERSION}_rbd)
