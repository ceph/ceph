include(Distutils)

set(CYTHON_MODULE_DIR ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cython_modules)

find_package(Cython REQUIRED)

add_subdirectory(rados)
add_custom_target(cython_modules ALL
  DEPENDS cython_rados)

if(WITH_RBD)
  add_subdirectory(rbd)
  add_dependencies(cython_modules cython_rbd)
endif()
if(WITH_LIBCEPHFS)
  add_subdirectory(cephfs)
  add_dependencies(cython_modules cython_cephfs)
endif()
if(WITH_RADOSGW)
  add_subdirectory(rgw)
  add_dependencies(cython_modules cython_rgw)
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
  ${Python3_EXECUTABLE} -c
    "import sysconfig;\
     print(\
     sysconfig.get_path(\
     scheme='deb_system' in sysconfig.get_scheme_names() and 'deb_system' or 'posix_prefix',\
     name='purelib',\
     vars=\
     {'base': '${PYTHON_INSTALL_TEMPLATE}',\
      'py_version_short': sysconfig.get_config_var('py_version_short')}))"
  OUTPUT_VARIABLE "PYTHON3_INSTDIR"
  OUTPUT_STRIP_TRAILING_WHITESPACE)

install(FILES
  ceph_argparse.py
  ceph_daemon.py
  DESTINATION ${PYTHON3_INSTDIR})

if(WITH_MGR)
  execute_process(
    COMMAND ${Python3_EXECUTABLE} -c "import ssl; print('.'.join(map(str,ssl.OPENSSL_VERSION_INFO[0:3])))"
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
  add_subdirectory(mgr)
endif(WITH_MGR)
