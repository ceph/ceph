function(build_curl)
  # only build the http bits
  list(APPEND curl_CMAKE_ARGS -DHTTP_ONLY=ON)

  list(APPEND curl_CMAKE_ARGS -DBUILD_SHARED_LIBS=OFF)

  list(APPEND curl_DEPENDS boringssl_ext)
  list(APPEND curl_CMAKE_ARGS -DOPENSSL_ROOT_DIR=${BORINGSSL_LIBRARY_DIR})
  list(APPEND curl_CMAKE_ARGS -DOPENSSL_INCLUDE_DIR=${BORINGSSL_INCLUDE_DIR})

  list(APPEND curl_DEPENDS quiche_ext)
  list(APPEND curl_CMAKE_ARGS -DUSE_QUICHE=ON)
  list(APPEND curl_CMAKE_ARGS -DQUICHE_LIBRARY=${QUICHE_LIBRARY})
  list(APPEND curl_CMAKE_ARGS -DQUICHE_INCLUDE_DIR=${QUICHE_INCLUDE_DIR})

  list(APPEND curl_CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER})
  list(APPEND curl_CMAKE_ARGS -DCMAKE_AR=${CMAKE_AR})
  list(APPEND curl_CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release)

  list(APPEND curl_CMAKE_ARGS -DCMAKE_RUNTIME_OUTPUT_DIRECTORY=${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

  set(curl_SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/curl/src")
  set(curl_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/curl/bin")
  set(curl_LIBRARY_DIR "${curl_BINARY_DIR}/lib")
  set(curl_LIBRARY "${curl_LIBRARY_DIR}/libcurl.a")
  set(curl_BINARY "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/curl")
  set(curl_BYPRODUCTS ${curl_LIBRARY} ${curl_BINARY})

  # resolve undefined reference to `fmod'
  set(make_command ${CMAKE_COMMAND} -E env LDFLAGS="-lm" ${CMAKE_COMMAND})

  include(ExternalProject)
  ExternalProject_Add(curl_ext
    SOURCE_DIR "${curl_SOURCE_DIR}"
    GIT_REPOSITORY https://github.com/curl/curl
    GIT_TAG curl-8_5_0
    GIT_SHALLOW ON
    UPDATE_COMMAND ""
    CMAKE_ARGS ${curl_CMAKE_ARGS}
    CMAKE_COMMAND ${make_command}
    BINARY_DIR "${curl_BINARY_DIR}"
    BUILD_BYPRODUCTS "${curl_BYPRODUCTS}"
    INSTALL_COMMAND ""
    DEPENDS "${curl_DEPENDS}"
    LIST_SEPARATOR !)

  set(CURL_INCLUDE_DIR "${curl_SOURCE_DIR}/include" PARENT_SCOPE)
  set(CURL_LIBRARY_DIR "${curl_LIBRARY_DIR}" PARENT_SCOPE)
  set(CURL_LIBRARY "${curl_LIBRARY}" PARENT_SCOPE)
endfunction()
