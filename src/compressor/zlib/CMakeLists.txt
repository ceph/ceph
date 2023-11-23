# zlib

if(HAVE_INTEL_SSE4_1 AND HAVE_NASM_X64_AVX2 AND (NOT APPLE))
  set(CMAKE_ASM_FLAGS "-i ${PROJECT_SOURCE_DIR}/src/isa-l/igzip/ -i ${PROJECT_SOURCE_DIR}/src/isa-l/include/ ${CMAKE_ASM_FLAGS}")
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
elseif(HAVE_ARMV8_SIMD)
  set(zlib_asm_sources
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/aarch64/igzip_inflate_multibinary_arm64.S
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/aarch64/igzip_multibinary_arm64.S
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/aarch64/igzip_deflate_body_aarch64.S
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/aarch64/igzip_deflate_finish_aarch64.S
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/aarch64/isal_deflate_icf_body_hash_hist.S
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/aarch64/isal_deflate_icf_finish_hash_hist.S
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/aarch64/igzip_set_long_icf_fg.S
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/aarch64/isal_update_histogram.S
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/aarch64/igzip_deflate_hash_aarch64.S
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/aarch64/igzip_decode_huffman_code_block_aarch64.S
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/aarch64/igzip_isal_adler32_neon.S
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/aarch64/encode_df.S
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/aarch64/gen_icf_map.S
  )
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
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/proc_heap_base.c
    ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/aarch64/igzip_multibinary_aarch64_dispatcher.c
    ${CMAKE_SOURCE_DIR}/src/isa-l/crc/crc_base_aliases.c
    ${CMAKE_SOURCE_DIR}/src/isa-l/crc/crc_base.c
    ${CMAKE_SOURCE_DIR}/src/isa-l/crc/crc64_base.c
    ${zlib_asm_sources}
  )
  set_source_files_properties(${zlib_asm_sources} PROPERTIES
    COMPILE_DEFINITIONS "__ASSEMBLY__"
    INCLUDE_DIRECTORIES "${PROJECT_SOURCE_DIR}/src/isa-l/igzip;${PROJECT_SOURCE_DIR}/src/isa-l/igzip/aarch64"
  )
else()
  set(zlib_sources
    CompressionPluginZlib.cc
    ZlibCompressor.cc
  )
endif()

add_library(ceph_zlib SHARED ${zlib_sources})
target_link_libraries(ceph_zlib ZLIB::ZLIB compressor $<$<PLATFORM_ID:Windows>:ceph-common>)
if(HAVE_QATZIP AND HAVE_QAT)
  target_link_libraries(ceph_zlib qat_compressor)
endif()
target_include_directories(ceph_zlib SYSTEM PRIVATE "${CMAKE_SOURCE_DIR}/src/isa-l/include")
set_target_properties(ceph_zlib PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_zlib DESTINATION ${compressor_plugin_dir})
