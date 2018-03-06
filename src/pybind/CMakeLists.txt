include(Distutils)

set(CYTHON_MODULE_DIR ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cython_modules)

# Keep the default version last
if(WITH_PYTHON3)
  list(APPEND py_vers 3)
endif()
if(WITH_PYTHON2)
  list(APPEND py_vers 2)
endif()
if(NOT py_vers)
  message(FATAL_ERROR "No Python binding enabled. Please enable WITH_PYTHON2 and/or WITH_PYTHON3.")
endif()

foreach(python_version ${py_vers})
  if(${python_version} EQUAL 2)
    set(PYTHON_VERSION "")
  else(${python_version} EQUAL 2)
    set(PYTHON_VERSION ${python_version})
  endif(${python_version} EQUAL 2)

  find_package(Cython REQUIRED)

  add_subdirectory(rados rados${PYTHON_VERSION})
  add_custom_target(cython${PYTHON_VERSION}_modules ALL
    DEPENDS cython${PYTHON_VERSION}_rados)

  if(WITH_RBD)
    add_subdirectory(rbd rbd${PYTHON_VERSION})
    add_dependencies(cython${PYTHON_VERSION}_modules cython${PYTHON_VERSION}_rbd)
  endif()
  if(WITH_CEPHFS)
    add_subdirectory(cephfs cephfs${PYTHON_VERSION})
    add_dependencies(cython${PYTHON_VERSION}_modules cython${PYTHON_VERSION}_cephfs)
  endif()
  if(WITH_RADOSGW)
    add_subdirectory(rgw rgw${PYTHON_VERSION})
    add_dependencies(cython${PYTHON_VERSION}_modules cython${PYTHON_VERSION}_rgw)
  endif()

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
    ${PYTHON${PYTHON_VERSION}_EXECUTABLE} -c "from distutils import sysconfig; print(sysconfig.get_python_lib(prefix='${PYTHON_INSTALL_TEMPLATE}'))"
    OUTPUT_VARIABLE "PYTHON${PYTHON_VERSION}_INSTDIR"
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/ceph_argparse.py
    ${CMAKE_CURRENT_SOURCE_DIR}/ceph_daemon.py
    ${CMAKE_CURRENT_SOURCE_DIR}/ceph_volume_client.py
    DESTINATION ${PYTHON${PYTHON_VERSION}_INSTDIR})
endforeach()

if(WITH_MGR)
  if(NOT WITH_PYTHON2 AND MGR_PYTHON_VERSION_MAJOR EQUAL 2)
    message(FATAL_ERROR "mgr plugins require python2 binding")
  elseif(NOT WITH_PYTHON3 AND MGR_PYTHON_VERSION_MAJOR EQUAL 3)
    message(FATAL_ERROR "mgr plugins require python3 binding")
  endif()
  # Location needs to match default setting for mgr_module_path, currently:
  # OPTION(mgr_module_path, OPT_STR, CEPH_PKGLIBDIR "/mgr")
  install(DIRECTORY
    ${CMAKE_CURRENT_SOURCE_DIR}/mgr
    DESTINATION ${CMAKE_INSTALL_PKGLIBDIR}
    REGEX ".*/\\.gitignore" EXCLUDE)
  add_subdirectory(mgr)
endif(WITH_MGR)
