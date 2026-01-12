# mylz4

set(mylz4_sources
  CompressionPluginMyLZ4.cc
)

add_library(ceph_mylz4 SHARED ${mylz4_sources})

# 【关键修改】这里必须链接真实的 LZ4::LZ4 库，而不是 MyLZ4
target_link_libraries(ceph_mylz4
  PRIVATE 
    LZ4::LZ4 
    compressor 
    $<$<PLATFORM_ID:Windows>:ceph-common>
)

set_target_properties(ceph_mylz4 PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")

install(TARGETS ceph_mylz4 DESTINATION ${compressor_plugin_dir})