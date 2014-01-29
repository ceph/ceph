// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - distributed storage system
 *
 * Copyright (C) 2014 CERN/Switzerland
 *
 * Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
 *
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 */

// -----------------------------------------------------------------------------

#include "xor.h"

// -----------------------------------------------------------------------------

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <set>

// -----------------------------------------------------------------------------

void
vector_xor (vector_op_t* cw, vector_op_t* dw, int vector_words)
{
  for (int i = 0; i < vector_words; ++i) {
    *cw++ ^= *dw++;
  }
}

// -----------------------------------------------------------------------------

void
vector_assign (vector_op_t* cw, vector_op_t* dw, int vector_words)
{
  memcpy((void*) cw, (void*) dw, VECTOR_WORDSIZE * vector_words);
}

// -----------------------------------------------------------------------------

void
vector_sse_xor (const std::set<vector_op_t*> &data,
                vector_op_t* parity,
                unsigned size)
{
#ifdef __x86_64__
  assert(!(size % 64));
  unsigned char* p;
  int d, l;
  unsigned i;
  unsigned char* vbuf[256];
  i = 0;
  for (std::set<vector_op_t*>::const_iterator it = data.begin();
    it != data.end(); ++it) {
    vbuf[i] = (unsigned char*) *it;
    i++;
  }

  l = data.size();
  p = (unsigned char*) parity;

  for (i = 0; i < size; i += 64) {
    asm volatile("movdqa %0,%%xmm0" : : "m" (vbuf[0][i]));
    asm volatile("movdqa %0,%%xmm1" : : "m" (vbuf[0][i + 16]));
    asm volatile("movdqa %0,%%xmm2" : : "m" (vbuf[0][i + 32]));
    asm volatile("movdqa %0,%%xmm3" : : "m" (vbuf[0][i + 48]));
    /* accessing disks in backward order because the buffers */
    /* are also in backward order */
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
#endif
  return;
}
