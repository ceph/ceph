find_package(Cython REQUIRED)
include(Distutils)

set(CYTHON_MODULE_DIR ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cython_modules)

add_subdirectory(rados)
if(WITH_RBD)
  add_subdirectory(rbd)
endif(WITH_RBD)
if(WITH_CEPHFS)
  add_subdirectory(cephfs)
endif(WITH_CEPHFS)

add_custom_target(cython_modules ALL
  DEPENDS cython_rados)
if(WITH_RBD)
  add_dependencies(cython_modules cython_rbd)
endif(WITH_RBD)
if(WITH_CEPHFS)
  add_dependencies(cython_modules cython_cephfs)
endif(WITH_CEPHFS)

# if CMAKE_INSTALL_PREFIX is an empty string, must replace
# it with "/" to make PYTHON_INSTALL_TEMPLATE an absolute path to be
# consistent with all other installation paths.
if(CMAKE_INSTALL_PREFIX)
  set(PYTHON_INSTALL_TEMPLATE "${CMAKE_INSTALL_PREFIX}")
else(CMAKE_INSTALL_PREFIX)
  set(PYTHON_INSTALL_TEMPLATE "/")
endif(CMAKE_INSTALL_PREFIX)

execute_process(
  COMMAND
  ${PYTHON_EXECUTABLE} -c "from distutils import sysconfig; print sysconfig.get_python_lib(prefix='${PYTHON_INSTALL_TEMPLATE}')"
  OUTPUT_VARIABLE PYTHON_INSTDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE)

install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/ceph_argparse.py
  ${CMAKE_CURRENT_SOURCE_DIR}/ceph_daemon.py
  ${CMAKE_CURRENT_SOURCE_DIR}/ceph_rest_api.py
  ${CMAKE_CURRENT_SOURCE_DIR}/ceph_volume_client.py
  DESTINATION ${PYTHON_INSTDIR})
