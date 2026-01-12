set(hrac_sources
  CompressionPluginHrac.cc
  HracCompressor.cc
  hrac.cc
)

add_library(ceph_hrac SHARED ${hrac_sources})
target_compile_options(ceph_hrac PRIVATE -march=native -O3)

target_link_libraries(ceph_hrac
  PRIVATE 
    compressor 
    $<$<PLATFORM_ID:Windows>:ceph-common>
)

set_target_properties(ceph_hrac PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")

install(TARGETS ceph_hrac DESTINATION ${compressor_plugin_dir})