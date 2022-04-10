##
#  IPP wrapper for ceph
##

set(ipp_crypto_plugin_srcs
  ipp_crypto_accel.cc 
  ipp_crypto_plugin.cc
  ippcrypto.cc)

add_library(ceph_crypto_ipp SHARED ${ipp_crypto_plugin_srcs})

target_link_libraries(ceph_crypto_ipp PRIVATE ${IPPCRYPTO_LIBRARIES})

target_include_directories(ceph_crypto_ipp PRIVATE ${IPPCRYPTO_INCLUDE_DIRS})

add_dependencies(crypto_plugins ceph_crypto_ipp)

set_target_properties(ceph_crypto_ipp PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS ceph_crypto_ipp DESTINATION ${crypto_plugin_dir})
