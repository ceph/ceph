set(ISAL_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/isa-l_crypto)
set(ISAL_INCLUDE_DIR "${ISAL_SOURCE_DIR}/include")
set(ISAL_LIBRARY "${ISAL_SOURCE_DIR}/bin/isa-l_crypto.a")

include(FindMake)
find_make("MAKE_EXECUTABLE" "make_cmd")

# use an ExternalProject to build isa-l_crypto using its makefile
include(ExternalProject)
ExternalProject_Add(isal_crypto_ext
  SOURCE_DIR ${ISAL_SOURCE_DIR}
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${make_cmd} -f <SOURCE_DIR>/Makefile.unx
  BUILD_IN_SOURCE 1
  BUILD_BYPRODUCTS ${ISAL_LIBRARY}
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
  INTERFACE_INCLUDE_DIRECTORIES ${ISAL_INCLUDE_DIR}
  IMPORTED_LINK_INTERFACE_LANGUAGES "C"
  IMPORTED_LOCATION ${ISAL_LIBRARY})


set(isal_crypto_plugin_srcs
  isal_crypto_accel.cc 
  isal_crypto_plugin.cc)

if(HAVE_NASM_X64)
add_dependencies(crypto_plugins ceph_crypto_isal)
endif(HAVE_NASM_X64)

add_library(ceph_crypto_isal SHARED ${isal_crypto_plugin_srcs})
target_link_libraries(ceph_crypto_isal PRIVATE ISAL::Crypto spawn)

set_target_properties(ceph_crypto_isal PROPERTIES
  VERSION 1.0.0
  SOVERSION 1
  INSTALL_RPATH "")
install(TARGETS ceph_crypto_isal DESTINATION ${crypto_plugin_dir})

add_subdirectory(md5)
