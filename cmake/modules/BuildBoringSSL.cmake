# build BoringSSL as a dependency of Quiche and rgw_h3_quiche

function(build_boringssl)
  # this will be statically linked into the rgw_h3_quiche library
  list(APPEND boringssl_CMAKE_ARGS -DBUILD_SHARED_LIBS=OFF)
  list(APPEND boringssl_CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=ON)

  # cmake doesn't properly handle arguments containing ";", such as
  # CMAKE_PREFIX_PATH, for which reason we'll have to use some other separator.
  string(REPLACE ";" "!" CMAKE_PREFIX_PATH_ALT_SEP "${CMAKE_PREFIX_PATH}")
  list(APPEND boringssl_CMAKE_ARGS -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH_ALT_SEP})
  if(CMAKE_TOOLCHAIN_FILE)
    list(APPEND boringssl_CMAKE_ARGS
         -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE})
  endif()

  list(APPEND boringssl_CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER})
  list(APPEND boringssl_CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})
  list(APPEND boringssl_CMAKE_ARGS -DCMAKE_AR=${CMAKE_AR})
  list(APPEND boringssl_CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release)

  set(boringssl_SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/boringssl/src")
  set(boringssl_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/boringssl/bin")
  set(boringssl_LIBRARY_DIR "${boringssl_SOURCE_DIR}/build")
  set(boringssl_CRYPTO_LIBRARY "${boringssl_LIBRARY_DIR}/libcrypto.a")
  set(boringssl_SSL_LIBRARY "${boringssl_LIBRARY_DIR}/libssl.a")

  list(APPEND boringssl_BYPRODUCTS ${boringssl_CRYPTO_LIBRARY})
  list(APPEND boringssl_BYPRODUCTS ${boringssl_SSL_LIBRARY})

  # the Quiche build expects to find binaries under SOURCE_DIR/build, but
  # ExternalProject doesn't work well when BINARY_DIR is inside of SOURCE_DIR
  # because the download step deletes/recreates the SOURCE_DIR. as a
  # workaround, add an install step to copy the libraries into SOURCE_DIR/build
  set(install_cmd ${CMAKE_COMMAND} -E copy <BINARY_DIR>/libcrypto.a <BINARY_DIR>/libssl.a ${boringssl_LIBRARY_DIR})

  include(ExternalProject)
  ExternalProject_Add(boringssl_ext
    SOURCE_DIR "${boringssl_SOURCE_DIR}"
    GIT_REPOSITORY https://boringssl.googlesource.com/boringssl
    GIT_TAG f1c75347daa2ea81a941e953f2263e0a4d970c8d
    UPDATE_COMMAND ""
    BINARY_DIR "${boringssl_BINARY_DIR}"
    CMAKE_ARGS ${boringssl_CMAKE_ARGS}
    BUILD_BYPRODUCTS "${boringssl_BYPRODUCTS}"
    INSTALL_COMMAND "${install_cmd}"
    LIST_SEPARATOR !)

  set(BORINGSSL_SOURCE_DIR "${boringssl_SOURCE_DIR}" PARENT_SCOPE)
  set(BORINGSSL_INCLUDE_DIR "${boringssl_SOURCE_DIR}/src/include" PARENT_SCOPE)
  set(BORINGSSL_LIBRARY_DIR "${boringssl_LIBRARY_DIR}" PARENT_SCOPE)
  set(BORINGSSL_CRYPTO_LIBRARY "${boringssl_CRYPTO_LIBRARY}" PARENT_SCOPE)
  set(BORINGSSL_SSL_LIBRARY "${boringssl_SSL_LIBRARY}" PARENT_SCOPE)
endfunction()
