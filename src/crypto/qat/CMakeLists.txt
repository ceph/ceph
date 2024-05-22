##
# QAT wrapper for Ceph
##

set(qat_crypto_plugin_srcs
  qat_crypto_accel.cc
  qat_crypto_plugin.cc
  qcccrypto.cc)

add_library(ceph_crypto_qat SHARED ${qat_crypto_plugin_srcs})

add_dependencies(crypto_plugins ceph_crypto_qat)

target_link_libraries(ceph_crypto_qat PRIVATE
                      QAT::qat
                      QAT::usdm
                      Boost::context)

add_dependencies(crypto_plugins ceph_crypto_qat)
set_target_properties(ceph_crypto_qat PROPERTIES VERSION 1.0.0 SOVERSION 1)
install(TARGETS ceph_crypto_qat DESTINATION ${crypto_plugin_dir})
