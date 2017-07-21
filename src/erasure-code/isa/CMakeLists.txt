# ISA
set(isal_src_dir ${CMAKE_SOURCE_DIR}/src/isa-l)
include_directories(${isal_src_dir}/include)

set(isa_srcs
  ${isal_src_dir}/erasure_code/ec_base.c
  ${isal_src_dir}/erasure_code/gf_2vect_dot_prod_sse.asm
  ${isal_src_dir}/erasure_code/gf_3vect_dot_prod_sse.asm
  ${isal_src_dir}/erasure_code/gf_4vect_dot_prod_sse.asm
  ${isal_src_dir}/erasure_code/gf_5vect_dot_prod_sse.asm
  ${isal_src_dir}/erasure_code/gf_6vect_dot_prod_sse.asm
  ${isal_src_dir}/erasure_code/gf_vect_dot_prod_sse.asm
  ${isal_src_dir}/erasure_code/gf_2vect_mad_avx2.asm
  ${isal_src_dir}/erasure_code/gf_3vect_mad_avx2.asm
  ${isal_src_dir}/erasure_code/gf_4vect_mad_avx2.asm
  ${isal_src_dir}/erasure_code/gf_5vect_mad_avx2.asm
  ${isal_src_dir}/erasure_code/gf_6vect_mad_avx2.asm
  ${isal_src_dir}/erasure_code/gf_vect_mad_avx2.asm
  ${isal_src_dir}/erasure_code/ec_highlevel_func.c
  ${isal_src_dir}/erasure_code/gf_2vect_mad_avx.asm
  ${isal_src_dir}/erasure_code/gf_3vect_mad_avx.asm
  ${isal_src_dir}/erasure_code/gf_4vect_mad_avx.asm
  ${isal_src_dir}/erasure_code/gf_5vect_mad_avx.asm
  ${isal_src_dir}/erasure_code/gf_6vect_mad_avx.asm
  ${isal_src_dir}/erasure_code/gf_vect_mad_avx.asm
  ${isal_src_dir}/erasure_code/ec_multibinary.asm
  ${isal_src_dir}/erasure_code/gf_2vect_mad_sse.asm
  ${isal_src_dir}/erasure_code/gf_3vect_mad_sse.asm
  ${isal_src_dir}/erasure_code/gf_4vect_mad_sse.asm
  ${isal_src_dir}/erasure_code/gf_5vect_mad_sse.asm
  ${isal_src_dir}/erasure_code/gf_6vect_mad_sse.asm
  ${isal_src_dir}/erasure_code/gf_vect_mad_sse.asm
  ${isal_src_dir}/erasure_code/gf_2vect_dot_prod_avx2.asm
  ${isal_src_dir}/erasure_code/gf_3vect_dot_prod_avx2.asm
  ${isal_src_dir}/erasure_code/gf_4vect_dot_prod_avx2.asm
  ${isal_src_dir}/erasure_code/gf_5vect_dot_prod_avx2.asm
  ${isal_src_dir}/erasure_code/gf_6vect_dot_prod_avx2.asm
  ${isal_src_dir}/erasure_code/gf_vect_dot_prod_avx2.asm
  ${isal_src_dir}/erasure_code/gf_vect_mul_avx.asm
  ${isal_src_dir}/erasure_code/gf_2vect_dot_prod_avx.asm
  ${isal_src_dir}/erasure_code/gf_3vect_dot_prod_avx.asm
  ${isal_src_dir}/erasure_code/gf_4vect_dot_prod_avx.asm
  ${isal_src_dir}/erasure_code/gf_5vect_dot_prod_avx.asm
  ${isal_src_dir}/erasure_code/gf_6vect_dot_prod_avx.asm
  ${isal_src_dir}/erasure_code/gf_vect_dot_prod_avx.asm
  ${isal_src_dir}/erasure_code/gf_vect_mul_sse.asm
  ${isal_src_dir}/erasure_code/gf_2vect_dot_prod_avx512.asm
  ${isal_src_dir}/erasure_code/gf_2vect_mad_avx512.asm
  ${isal_src_dir}/erasure_code/gf_3vect_dot_prod_avx512.asm
  ${isal_src_dir}/erasure_code/gf_3vect_mad_avx512.asm
  ${isal_src_dir}/erasure_code/gf_4vect_dot_prod_avx512.asm
  ${isal_src_dir}/erasure_code/gf_4vect_mad_avx512.asm
  ${isal_src_dir}/erasure_code/gf_vect_dot_prod_avx512.asm
  ${isal_src_dir}/erasure_code/gf_vect_mad_avx512.asm
  ErasureCodeIsa.cc
  ErasureCodeIsaTableCache.cc
  ErasureCodePluginIsa.cc
  xor_op.cc
)

add_library(ec_isa SHARED
  ${isa_srcs}
  $<TARGET_OBJECTS:erasure_code_objs>)
add_dependencies(ec_isa ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_isa ${EXTRALIBS})
set_target_properties(ec_isa PROPERTIES
  INSTALL_RPATH "")
install(TARGETS ec_isa DESTINATION ${erasure_plugin_dir})

if(WITH_EMBEDDED)
  add_library(cephd_ec_isa STATIC ${isa_srcs})
  set_target_properties(cephd_ec_isa PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
endif()
