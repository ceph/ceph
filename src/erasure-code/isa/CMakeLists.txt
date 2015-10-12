# ISA

include_directories(isa-l/include)

set(isa_srcs
  isa-l/erasure_code/ec_base.c
  isa-l/erasure_code/gf_2vect_dot_prod_sse.asm.s
  isa-l/erasure_code/gf_3vect_dot_prod_sse.asm.s
  isa-l/erasure_code/gf_4vect_dot_prod_sse.asm.s
  isa-l/erasure_code/gf_5vect_dot_prod_sse.asm.s
  isa-l/erasure_code/gf_6vect_dot_prod_sse.asm.s
  isa-l/erasure_code/gf_vect_dot_prod_sse.asm.s
  isa-l/erasure_code/gf_2vect_mad_avx2.asm.s
  isa-l/erasure_code/gf_3vect_mad_avx2.asm.s
  isa-l/erasure_code/gf_4vect_mad_avx2.asm.s
  isa-l/erasure_code/gf_5vect_mad_avx2.asm.s
  isa-l/erasure_code/gf_6vect_mad_avx2.asm.s
  isa-l/erasure_code/gf_vect_mad_avx2.asm.s
  isa-l/erasure_code/ec_highlevel_func.c
  isa-l/erasure_code/gf_2vect_mad_avx.asm.s
  isa-l/erasure_code/gf_3vect_mad_avx.asm.s
  isa-l/erasure_code/gf_4vect_mad_avx.asm.s
  isa-l/erasure_code/gf_5vect_mad_avx.asm.s
  isa-l/erasure_code/gf_6vect_mad_avx.asm.s
  isa-l/erasure_code/gf_vect_mad_avx.asm.s
  isa-l/erasure_code/ec_multibinary.asm.s
  isa-l/erasure_code/gf_2vect_mad_sse.asm.s
  isa-l/erasure_code/gf_3vect_mad_sse.asm.s
  isa-l/erasure_code/gf_4vect_mad_sse.asm.s
  isa-l/erasure_code/gf_5vect_mad_sse.asm.s
  isa-l/erasure_code/gf_6vect_mad_sse.asm.s
  isa-l/erasure_code/gf_vect_mad_sse.asm.s
  isa-l/erasure_code/gf_2vect_dot_prod_avx2.asm.s
  isa-l/erasure_code/gf_3vect_dot_prod_avx2.asm.s
  isa-l/erasure_code/gf_4vect_dot_prod_avx2.asm.s
  isa-l/erasure_code/gf_5vect_dot_prod_avx2.asm.s
  isa-l/erasure_code/gf_6vect_dot_prod_avx2.asm.s
  isa-l/erasure_code/gf_vect_dot_prod_avx2.asm.s
  isa-l/erasure_code/gf_vect_mul_avx.asm.s
  isa-l/erasure_code/gf_2vect_dot_prod_avx.asm.s
  isa-l/erasure_code/gf_3vect_dot_prod_avx.asm.s
  isa-l/erasure_code/gf_4vect_dot_prod_avx.asm.s
  isa-l/erasure_code/gf_5vect_dot_prod_avx.asm.s
  isa-l/erasure_code/gf_6vect_dot_prod_avx.asm.s
  isa-l/erasure_code/gf_vect_dot_prod_avx.asm.s
  isa-l/erasure_code/gf_vect_mul_sse.asm.s
  ErasureCodeIsa.cc
  ErasureCodeIsaTableCache.cc
  ErasureCodePluginIsa.cc
  xor_op.cc
  $<TARGET_OBJECTS:erasure_code_objs>
)

add_library(ec_isa SHARED ${isa_srcs})
add_dependencies(ec_isa ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_isa ${EXTRALIBS})
set_target_properties(ec_isa PROPERTIES VERSION 2.14.0 SOVERSION 2)
install(TARGETS ec_isa DESTINATION lib/erasure-code)
