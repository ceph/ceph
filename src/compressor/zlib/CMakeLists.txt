# zlib

if(HAVE_INTEL_SSE4_1 AND HAVE_BETTER_YASM_ELF64 AND (NOT APPLE))
	set(zlib_sources
	  CompressionPluginZlib.cc
	  ZlibCompressor.cc
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/hufftables_c.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_base.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_icf_base.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/adler32_base.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/flatten_ll.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/encode_df.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_icf_body.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_inflate.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/huff_codes.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/crc/crc_base_aliases.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/crc/crc_base.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/crc/crc64_base.c
	)
	list(APPEND zlib_sources
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_body.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_finish.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_icf_body_h1_gr_bt.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_icf_finish.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/rfc1951_lookup.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/adler32_sse.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/adler32_avx2_4.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_multibinary.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_update_histogram_01.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_update_histogram_04.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_decode_block_stateless_01.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_decode_block_stateless_04.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_inflate_multibinary.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/encode_df_04.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/encode_df_06.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/proc_heap.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_deflate_hash.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_gen_icf_map_lh1_06.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_gen_icf_map_lh1_04.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_set_long_icf_fg_04.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_set_long_icf_fg_06.asm
	)
else(HAVE_INTEL_SSE4_1 AND HAVE_BETTER_YASM_ELF64 AND (NOT APPLE))
	set(zlib_sources
	  CompressionPluginZlib.cc
	  ZlibCompressor.cc
	)
endif(HAVE_INTEL_SSE4_1 AND HAVE_BETTER_YASM_ELF64 AND (NOT APPLE))

add_library(ceph_zlib SHARED ${zlib_sources})
target_link_libraries(ceph_zlib ZLIB::ZLIB compressor)
target_include_directories(ceph_zlib SYSTEM PRIVATE "${CMAKE_SOURCE_DIR}/src/isa-l/include")
set_target_properties(ceph_zlib PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_zlib DESTINATION ${compressor_plugin_dir})
