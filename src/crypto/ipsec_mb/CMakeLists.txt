set(ipsec_dir ${CMAKE_SOURCE_DIR}/src/crypto/ipsec_mb/intel-ipsec-mb/lib)
set(CMAKE_ASM_FLAGS "-i ${ipsec_dir}/include/ ${CMAKE_ASM_FLAGS}")

add_custom_target(ipsec_mb_so ALL
	COMMAND bash -c "make OBJ_DIR=${CMAKE_BINARY_DIR}/src/crypto/ipsec_mb/obj LIB_DIR=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}"
	WORKING_DIRECTORY ${ipsec_dir}
	COMMENT "build libIPSec_MB.so")

set(ipsec_crypto_plugin_srcs
  ipsec_crypto_accel.cc
  ipsec_crypto_plugin.cc)

if(HAVE_NASM_X64)
add_dependencies(crypto_plugins ceph_crypto_ipsec)
endif(HAVE_NASM_X64)

link_directories(${CMAKE_LIBRARY_OUTPUT_DIRECTORY})

add_library(ceph_crypto_ipsec SHARED ${ipsec_crypto_plugin_srcs})
add_dependencies(ceph_crypto_ipsec ipsec_mb_so)
target_link_libraries(ceph_crypto_ipsec IPSec_MB)
target_include_directories(ceph_crypto_ipsec PRIVATE ${ipsec_dir}/include)
set_target_properties(ceph_crypto_ipsec PROPERTIES
  VERSION 1.0.0
  SOVERSION 1
  INSTALL_RPATH "")
install(TARGETS ceph_crypto_ipsec DESTINATION ${crypto_plugin_dir})
install(FILES
	${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libIPSec_MB.so.0.55.0
	${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libIPSec_MB.so.0
	${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libIPSec_MB.so
	DESTINATION ${CMAKE_INSTALL_LIBDIR})
