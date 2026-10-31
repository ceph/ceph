# build isa-l_crypto from its makefile and expose as target ISAL::Crypto
include(BuildISALCrypto)
build_isal_crypto()

set(isal_crypto_plugin_srcs
  isal_crypto_accel.cc 
  isal_crypto_plugin.cc)

if(HAVE_NASM_X64)
add_dependencies(crypto_plugins ceph_crypto_isal)
endif(HAVE_NASM_X64)

add_library(ceph_crypto_isal SHARED ${isal_crypto_plugin_srcs})
target_link_libraries(ceph_crypto_isal PRIVATE ISAL::Crypto Boost::context)

set_target_properties(ceph_crypto_isal PROPERTIES
  VERSION 1.0.0
  SOVERSION 1
  INSTALL_RPATH "")
install(TARGETS ceph_crypto_isal DESTINATION ${crypto_plugin_dir})
