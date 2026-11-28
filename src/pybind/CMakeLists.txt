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
  # ceph-mgr loads libcrypto before embedded Python imports ssl.
  if(NOT CMAKE_CROSSCOMPILING AND NOT OPENSSL_CRYPTO_LIBRARY MATCHES "\\.a$")
    execute_process(
      COMMAND ${Python3_EXECUTABLE} -S -c
        "import ctypes, sys; ctypes.CDLL(sys.argv[1], mode=ctypes.RTLD_GLOBAL); import ssl; ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)"
        "${OPENSSL_CRYPTO_LIBRARY}"
      RESULT_VARIABLE PYSSL_RESULT
      ERROR_VARIABLE PYSSL_ERROR
      OUTPUT_QUIET)
    if(NOT PYSSL_RESULT STREQUAL "0")
      message(FATAL_ERROR "Python ssl cannot use Ceph's OpenSSL library ${OPENSSL_CRYPTO_LIBRARY}: ${PYSSL_ERROR}")
    endif()
  endif()
  add_subdirectory(mgr)
endif(WITH_MGR)
