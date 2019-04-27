## openssl

set(openssl_crypto_plugin_srcs
  openssl_crypto_accel.cc
  openssl_crypto_plugin.cc)

add_library(ceph_crypto_openssl SHARED ${openssl_crypto_plugin_srcs})
target_link_libraries(ceph_crypto_openssl PRIVATE crypto)
add_dependencies(crypto_plugins ceph_crypto_openssl)
install(TARGETS ceph_crypto_openssl DESTINATION ${crypto_plugin_dir})
