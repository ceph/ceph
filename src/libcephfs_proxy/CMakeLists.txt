set(proxy_common_srcs proxy_helpers.c proxy_link.c proxy_async.c proxy_log.c)
set(libcephfsd_srcs libcephfsd.c proxy_manager.c proxy_mount.c ${proxy_common_srcs})
set(libcephfs_proxy_srcs libcephfs_proxy.c ${proxy_common_srcs})

add_executable(libcephfsd ${libcephfsd_srcs})
add_library(cephfs_proxy ${CEPH_SHARED} ${libcephfs_proxy_srcs})

target_link_libraries(libcephfsd cephfs ${CRYPTO_LIBS})
# proxy_helpers.c is in both targets and hashes with EVP, so both need
# OpenSSL - for its headers as much as its symbols, since they are not
# in the default include path everywhere.
target_link_libraries(cephfs_proxy ${CRYPTO_LIBS})

if(ENABLE_SHARED)
  set_target_properties(cephfs_proxy PROPERTIES
    OUTPUT_NAME cephfs_proxy
    VERSION 2.0.0
    SOVERSION 2)
endif(ENABLE_SHARED)

install(TARGETS libcephfsd DESTINATION ${CMAKE_INSTALL_SBINDIR})
install(TARGETS cephfs_proxy DESTINATION ${CMAKE_INSTALL_LIBDIR})
