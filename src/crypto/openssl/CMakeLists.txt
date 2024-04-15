## openssl

set(openssl_crypto_plugin_srcs
  openssl_crypto_accel.cc
  openssl_crypto_plugin.cc)

add_library(ceph_crypto_openssl SHARED ${openssl_crypto_plugin_srcs})
add_dependencies(ceph_crypto_openssl prebuild-targets)
target_link_libraries(ceph_crypto_openssl
    PRIVATE OpenSSL::Crypto
    $<$<PLATFORM_ID:Windows>:ceph-common>
    spawn)
target_include_directories(ceph_crypto_openssl PRIVATE ${OPENSSL_INCLUDE_DIR})
add_dependencies(crypto_plugins ceph_crypto_openssl)
set_target_properties(ceph_crypto_openssl PROPERTIES INSTALL_RPATH "")
install(TARGETS ceph_crypto_openssl DESTINATION ${crypto_plugin_dir})
