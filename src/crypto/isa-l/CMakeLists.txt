set(isal_dir ${CMAKE_SOURCE_DIR}/src/crypto/isa-l/isa-l_crypto)

set(isal_crypto_plugin_srcs
  isal_crypto_accel.cc 
  isal_crypto_plugin.cc
  ${isal_dir}/aes/cbc_pre.c
  ${isal_dir}/aes/cbc_multibinary.asm
  ${isal_dir}/aes/keyexp_128.asm
  ${isal_dir}/aes/keyexp_192.asm
  ${isal_dir}/aes/keyexp_256.asm
  ${isal_dir}/aes/keyexp_multibinary.asm
  ${isal_dir}/aes/cbc_dec_128_x4_sse.asm
  ${isal_dir}/aes/cbc_dec_128_x8_avx.asm
  ${isal_dir}/aes/cbc_dec_192_x4_sse.asm
  ${isal_dir}/aes/cbc_dec_192_x8_avx.asm
  ${isal_dir}/aes/cbc_dec_256_x4_sse.asm
  ${isal_dir}/aes/cbc_dec_256_x8_avx.asm
  ${isal_dir}/aes/cbc_enc_128_x4_sb.asm
  ${isal_dir}/aes/cbc_enc_128_x8_sb.asm
  ${isal_dir}/aes/cbc_enc_192_x4_sb.asm
  ${isal_dir}/aes/cbc_enc_192_x8_sb.asm
  ${isal_dir}/aes/cbc_enc_256_x4_sb.asm
  ${isal_dir}/aes/cbc_enc_256_x8_sb.asm)

add_library(isal_crypto_plugin_objs OBJECT ${isal_crypto_plugin_srcs})
target_include_directories(isal_crypto_plugin_objs PRIVATE ${isal_dir}/include)

if(HAVE_GOOD_YASM_ELF64)
add_dependencies(crypto_plugins ceph_crypto_isal)
endif(HAVE_GOOD_YASM_ELF64)

add_library(ceph_crypto_isal SHARED ${isal_crypto_plugin_srcs})
target_include_directories(ceph_crypto_isal PRIVATE ${isal_dir}/include)
set_target_properties(ceph_crypto_isal PROPERTIES
  VERSION 1.0.0
  SOVERSION 1
  INSTALL_RPATH "")
install(TARGETS ceph_crypto_isal DESTINATION ${crypto_plugin_dir})
