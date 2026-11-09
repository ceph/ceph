# zstd

set(zstd_sources
  CompressionPluginZstd.cc)

add_library(ceph_zstd SHARED ${zstd_sources})
target_link_libraries(ceph_zstd PRIVATE
  legacy-option-headers
  Zstd::Zstd $<$<PLATFORM_ID:Windows>:ceph-common>)
if(NOT WITH_SYSTEM_ZSTD AND NOT APPLE AND NOT WIN32)
  # The bundled libzstd.a is linked in statically, but its symbols would
  # still be exported. In a daemon the distro libzstd.so.1 is usually loaded
  # as well (libcurl needs it), and the plugin's calls into zstd would then
  # bind to that library instead of the bundled one. Keep them local.
  # ld64 and the MinGW lld have no --exclude-libs; a Windows DLL binds its
  # imports per DLL, so the problem does not arise there.
  target_link_options(ceph_zstd PRIVATE "LINKER:--exclude-libs,libzstd.a")
endif()
set_target_properties(ceph_zstd PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_zstd DESTINATION ${compressor_plugin_dir})
