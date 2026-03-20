# use an ExternalProject to build isa-l_crypto using its makefile
function(build_isal_crypto)
  set(ISAL_CRYPTO_SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/crypto/isa-l/isa-l_crypto)
  set(ISAL_CRYPTO_INCLUDE_DIR "${ISAL_CRYPTO_SOURCE_DIR}/include")
  set(ISAL_CRYPTO_LIBRARY "${ISAL_CRYPTO_SOURCE_DIR}/bin/isa-l_crypto.a")

  include(FindMake)
  find_make("MAKE_EXECUTABLE" "make_cmd")

  include(ExternalProject)
  ExternalProject_Add(isal_crypto_ext
    SOURCE_DIR ${ISAL_CRYPTO_SOURCE_DIR}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ${make_cmd} -f <SOURCE_DIR>/Makefile.unx
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS ${ISAL_CRYPTO_LIBRARY}
    INSTALL_COMMAND ""
    UPDATE_COMMAND ""
    LOG_CONFIGURE ON
    LOG_BUILD ON
    LOG_MERGED_STDOUTERR ON
    LOG_OUTPUT_ON_FAILURE ON)

  # add imported library target ISAL::Crypto
  add_library(ISAL::Crypto STATIC IMPORTED GLOBAL)
  add_dependencies(ISAL::Crypto isal_crypto_ext)
  set_target_properties(ISAL::Crypto PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${ISAL_CRYPTO_INCLUDE_DIR}
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION ${ISAL_CRYPTO_LIBRARY})
endfunction()
