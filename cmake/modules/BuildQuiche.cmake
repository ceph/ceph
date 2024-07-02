# build Quiche as a dependency of rgw_h3_quiche

function(build_quiche)
  if(NOT BORINGSSL_SOURCE_DIR)
    message(FATAL_ERROR "BuildQuiche requires BORINGSSL_SOURCE_DIR from BuildBoringSSL")
  endif()
  list(APPEND quiche_DEPENDS boringssl_ext)

  # build the library with cargo, but invoke it with cmake -E env
  # to pass in the boringssl path
  set(cargo_command ${CMAKE_COMMAND} -E env QUICHE_BSSL_PATH=${BORINGSSL_SOURCE_DIR} cargo build)

  list(APPEND cargo_args --package quiche)
  list(APPEND cargo_args --features ffi,pkg-config-meta,qlog)
  list(APPEND cargo_args --release)

  set(quiche_SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/quiche")
  set(quiche_BINARY_DIR "${quiche_SOURCE_DIR}")
  set(quiche_LIBRARY_DIR "${quiche_BINARY_DIR}/target/release")
  set(quiche_LIBRARY "${quiche_LIBRARY_DIR}/libquiche.a")
  set(quiche_PC "${quiche_LIBRARY_DIR}/quiche.pc")
  set(quiche_BYPRODUCTS ${quiche_LIBRARY} ${quiche_PC})

  include(ExternalProject)
  ExternalProject_Add(quiche_ext
    SOURCE_DIR "${quiche_SOURCE_DIR}"
    GIT_REPOSITORY https://github.com/cloudflare/quiche
    GIT_TAG 0.21.0
    GIT_SHALLOW TRUE
    GIT_SUBMODULES "" # don't clone its boringssl submodule
    UPDATE_COMMAND ""
    CONFIGURE_COMMAND ""
    BINARY_DIR "${quiche_BINARY_DIR}"
    BUILD_COMMAND ${cargo_command} ${cargo_args}
    BUILD_BYPRODUCTS "${quiche_BYPRODUCTS}"
    INSTALL_COMMAND ""
    DEPENDS "${quiche_DEPENDS}"
    LIST_SEPARATOR !)

  set(QUICHE_INCLUDE_DIR "${quiche_SOURCE_DIR}/quiche/include" PARENT_SCOPE)
  set(QUICHE_LIBRARY_DIR "${quiche_LIBRARY_DIR}" PARENT_SCOPE)
  set(QUICHE_LIBRARY "${quiche_LIBRARY}" PARENT_SCOPE)
endfunction()
