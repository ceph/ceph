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

  if(USE_OPENSSL)
    execute_process(
      COMMAND ${PYTHON${PYTHON_VERSION}_EXECUTABLE} -c "import ssl; print('.'.join(map(str,ssl.OPENSSL_VERSION_INFO[0:3])))"
      RESULT_VARIABLE PYSSL_RESULT
      OUTPUT_VARIABLE PYSSL_VER
      ERROR_QUIET)
    if (NOT ${PYSSL_RESULT})
      # the ideal way to get the soversion is to parse the suffix of file name of
      # `/lib/x86_64-linux-gnu/libssl.so.1.0.0`, but since we're lazy, and will just
      # trust the version number here.
      macro(get_openssl_soversion version prefix)
        string(REPLACE "." ";" ssl_version_list ${version})
        list(GET ssl_version_list 0 ssl_version_major)
        list(GET ssl_version_list 1 ssl_version_minor)
        set(${prefix}_SOVERSION ${ssl_version_major}.${ssl_version_minor})
        unset(ssl_version_list)
        unset(ssl_version_major)
        unset(ssl_version_minor)
      endmacro()
      get_openssl_soversion(${OPENSSL_VERSION} OPENSSL)
      get_openssl_soversion(${PYSSL_VER} PYSSL)
      if(NOT (OPENSSL_SOVERSION VERSION_EQUAL PYSSL_SOVERSION))
        message(FATAL_ERROR "Python and Ceph link to different OpenSSL versions: ${PYSSL_VER} vs ${OPENSSL_VERSION}")
      endif()
    else()
      message(WARNING "could not determine ssl version of python crypto lib")
    endif()
  endif(USE_OPENSSL)
  add_subdirectory(mgr)
endif(WITH_MGR)
