/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CERN (Switzerland)
 *                                                                                                                                                                                                            * Author: Andreas-Joachim Peters <Andreas.Joachim.Peters@cern.ch>                                                                                                                                            *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

// -----------------------------------------------------------------------------
#include "xor_op.h"
#include <stdio.h>
#include <string.h>
#include "arch/intel.h"
#include "arch/arm.h"

#if defined(__aarch64__) && defined(__ARM_NEON)
  #include <arm_neon.h>
#endif

#include "include/ceph_assert.h"

// -----------------------------------------------------------------------------


// -----------------------------------------------------------------------------

void
// -----------------------------------------------------------------------------
byte_xor(unsigned char* cw, unsigned char* dw, unsigned char* ew)
// -----------------------------------------------------------------------------
{
  while (cw < ew)
    *dw++ ^= *cw++;
}

// -----------------------------------------------------------------------------

void
// -----------------------------------------------------------------------------
vector_xor(vector_op_t* cw,
           vector_op_t* dw,
           vector_op_t* ew)
// -----------------------------------------------------------------------------
{
  ceph_assert(is_aligned(cw, EC_ISA_VECTOR_OP_WORDSIZE));
  ceph_assert(is_aligned(dw, EC_ISA_VECTOR_OP_WORDSIZE));
  ceph_assert(is_aligned(ew, EC_ISA_VECTOR_OP_WORDSIZE));
  while (cw < ew) {
    *dw++ ^= *cw++;
  }
}


// -----------------------------------------------------------------------------

void
// -----------------------------------------------------------------------------
region_xor(unsigned char** src,
           unsigned char* parity,
           int src_size,
           unsigned size)
{
  if (!size) {
    // nothing to do
    return;
  }

  if (!src_size) {
    // nothing to do
    return;
  }

  if (src_size == 1) {
    // just copy source to parity
    memcpy(parity, src[0], size);
    return;
  }

  unsigned size_left = size;

  // ----------------------------------------------------------
  // region or vector XOR operations require aligned addresses
  // ----------------------------------------------------------

  bool src_aligned = true;
  for (int i = 0; i < src_size; i++) {
    src_aligned &= is_aligned(src[i], EC_ISA_VECTOR_OP_WORDSIZE);
  }

  if (src_aligned &&
      is_aligned(parity, EC_ISA_VECTOR_OP_WORDSIZE)) {

#ifdef __x86_64__
    if (ceph_arch_intel_sse2) {
      // -----------------------------
      // use SSE2 region xor function
      // -----------------------------
      unsigned region_size =
        (size / EC_ISA_VECTOR_SSE2_WORDSIZE) * EC_ISA_VECTOR_SSE2_WORDSIZE;

      size_left -= region_size;
      // 64-byte region xor
      region_sse2_xor((char**) src, (char*) parity, src_size, region_size);
    } else
#elif defined (__aarch64__) && defined(__ARM_NEON)
    if (ceph_arch_neon) {
      // -----------------------------
      // use NEON region xor function
      // -----------------------------
      unsigned region_size = 
        (size / EC_ISA_VECTOR_NEON_WORDSIZE) * EC_ISA_VECTOR_NEON_WORDSIZE;
      size_left -= region_size;
      region_neon_xor((char**) src, (char *) parity, src_size, region_size);
    } else
#endif
    {
      // --------------------------------------------
      // use region xor based on vector xor operation
      // --------------------------------------------
      unsigned vector_words = size / EC_ISA_VECTOR_OP_WORDSIZE;
      unsigned vector_size = vector_words * EC_ISA_VECTOR_OP_WORDSIZE;
      memcpy(parity, src[0], vector_size);

      size_left -= vector_size;
      vector_op_t* p_vec = (vector_op_t*) parity;
      for (int i = 1; i < src_size; i++) {
        vector_op_t* s_vec = (vector_op_t*) src[i];
        vector_op_t* e_vec = s_vec + vector_words;
        vector_xor(s_vec, p_vec, e_vec);
      }
    }
  }

  if (size_left) {
    // --------------------------------------------------
    // xor the not aligned part with byte-wise region xor
    // --------------------------------------------------
    memcpy(parity + size - size_left, src[0] + size - size_left, size_left);
    for (int i = 1; i < src_size; i++) {
      byte_xor(src[i] + size - size_left, parity + size - size_left, src[i] + size);
    }
  }
}

// -----------------------------------------------------------------------------

void
// -----------------------------------------------------------------------------
region_sse2_xor(char** src,
                char* parity,
                int src_size,
                unsigned size)
// -----------------------------------------------------------------------------
{
#ifdef __x86_64__
  ceph_assert(!(size % EC_ISA_VECTOR_SSE2_WORDSIZE));
  unsigned char* p;
  int d, l;
  unsigned i;
  unsigned char* vbuf[256];

  for (int v = 0; v < src_size; v++) {
    vbuf[v] = (unsigned char*) src[v];
  }

  l = src_size;
  p = (unsigned char*) parity;

  for (i = 0; i < size; i += EC_ISA_VECTOR_SSE2_WORDSIZE) {
    asm volatile("movdqa %0,%%xmm0" : : "m" (vbuf[0][i]));
    asm volatile("movdqa %0,%%xmm1" : : "m" (vbuf[0][i + 16]));
    asm volatile("movdqa %0,%%xmm2" : : "m" (vbuf[0][i + 32]));
    asm volatile("movdqa %0,%%xmm3" : : "m" (vbuf[0][i + 48]));

    for (d = 1; d < l; d++) {
      asm volatile("movdqa %0,%%xmm4" : : "m" (vbuf[d][i]));
      asm volatile("movdqa %0,%%xmm5" : : "m" (vbuf[d][i + 16]));
      asm volatile("movdqa %0,%%xmm6" : : "m" (vbuf[d][i + 32]));
      asm volatile("movdqa %0,%%xmm7" : : "m" (vbuf[d][i + 48]));
      asm volatile("pxor %xmm4,%xmm0");
      asm volatile("pxor %xmm5,%xmm1");
      asm volatile("pxor %xmm6,%xmm2");
      asm volatile("pxor %xmm7,%xmm3");
    }
    asm volatile("movntdq %%xmm0,%0" : "=m" (p[i]));
    asm volatile("movntdq %%xmm1,%0" : "=m" (p[i + 16]));
    asm volatile("movntdq %%xmm2,%0" : "=m" (p[i + 32]));
    asm volatile("movntdq %%xmm3,%0" : "=m" (p[i + 48]));
  }

  asm volatile("sfence" : : : "memory");
#endif // __x86_64__
  return;
}

#if defined(__aarch64__) && defined(__ARM_NEON)
void
// -----------------------------------------------------------------------------
region_neon_xor(char **src,
                char *parity,
                int src_size,
                unsigned size)
// -----------------------------------------------------------------------------
{
  ceph_assert(!(size % EC_ISA_VECTOR_NEON_WORDSIZE));
  unsigned char *p = (unsigned char *)parity;
  unsigned char *vbuf[256] = { NULL };
  for (int v = 0; v < src_size; v++) {
    vbuf[v] = (unsigned char *)src[v];
  }

  // ----------------------------------------------------------------------------------------
  // NEON load instructions can load 128bits of data each time, and there are 2 load channels
  // ----------------------------------------------------------------------------------------
  for (unsigned i = 0; i < size; i += EC_ISA_VECTOR_NEON_WORDSIZE) {
    uint64x2_t d0_1 = vld1q_u64((uint64_t *)(&(vbuf[0][i])));
    uint64x2_t d0_2 = vld1q_u64((uint64_t *)(&(vbuf[0][i + 16])));

    for (int d = 1; d < src_size; d++) {
      uint64x2_t di_1 = vld1q_u64((uint64_t *)(&(vbuf[d][i])));
      uint64x2_t di_2 = vld1q_u64((uint64_t *)(&(vbuf[d][i + 16])));

      d0_1 = veorq_u64(d0_1, di_1);
      d0_2 = veorq_u64(d0_2, di_2);
    }

    vst1q_u64((uint64_t *)p, d0_1);
    vst1q_u64((uint64_t *)(p + 16), d0_2);
    p += EC_ISA_VECTOR_NEON_WORDSIZE;
  }
  return;
}
#endif // __aarch64__ && __ARM_NEON
