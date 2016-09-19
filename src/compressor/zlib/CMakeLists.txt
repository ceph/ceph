# zlib

if(INTEL_SSE4_1 AND HAVE_BETTER_YASM_ELF64)
	set(zlib_sources
	  CompressionPluginZlib.cc
	  ZlibCompressor.cc
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/hufftables_c.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/crc_utils_01.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/crc_utils_04.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_body_01.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_body_04.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_finish.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_stateless_01.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_stateless_04.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/crc_data.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/crc32_gzip.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/detect_repeated_char.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_multibinary.asm
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_stateless_base.c
	  ${CMAKE_SOURCE_DIR}/src/isa-l/igzip/igzip_base.c
	)
else(INTEL_SSE4_1 AND HAVE_BETTER_YASM_ELF64)
	set(zlib_sources
	  CompressionPluginZlib.cc
	  ZlibCompressor.cc
	)
endif(INTEL_SSE4_1 AND HAVE_BETTER_YASM_ELF64)

add_library(ceph_zlib SHARED ${zlib_sources})
add_dependencies(ceph_zlib ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_zlib z)
target_include_directories(ceph_zlib PRIVATE "${CMAKE_SOURCE_DIR}/src/isa-l/include")
set_target_properties(ceph_zlib PROPERTIES VERSION 2.0.0 SOVERSION 2)
install(TARGETS ceph_zlib DESTINATION ${compressor_plugin_dir})
