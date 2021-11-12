# zstd

option(WITH_SYSTEM_ZSTD "use prebuilt libzstd in system" OFF)

if(WITH_SYSTEM_ZSTD)
  find_package(Zstd 1.4.4 REQUIRED)
else()
  include(BuildZstd)
  build_Zstd()
endif()

set(zstd_sources
  CompressionPluginZstd.cc)

add_library(ceph_zstd SHARED ${zstd_sources})
target_link_libraries(ceph_zstd PRIVATE Zstd::Zstd $<$<PLATFORM_ID:Windows>:ceph-common>)
set_target_properties(ceph_zstd PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_zstd DESTINATION ${compressor_plugin_dir})
