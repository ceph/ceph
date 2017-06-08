/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CERN (Switzerland)
 *                                                                                                                                                                                                           \
 * Author: Andreas-Joachim Peters <Andreas.Joachim.Peters@cern.ch>                                                                                                                                           \
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef EC_ISA_XOR_OP_H
#define EC_ISA_XOR_OP_H

// -----------------------------------------------------------------------------
#include <assert.h>
#include <stdint.h>
#include <string.h>
// -----------------------------------------------------------------------------

// -------------------------------------------------------------------------
// declaration of 64/128-bit vector operations depending on availability
// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

#define EC_ISA_ADDRESS_ALIGNMENT 32u
#define EC_ISA_VECTOR_SSE2_WORDSIZE 64u

#if __GNUC__ > 4 || \
  ( (__GNUC__ == 4) && (__GNUC_MINOR__ >= 4) ) ||\
  (__clang__ == 1 )
#ifdef EC_ISA_VECTOR_OP_DEBUG
#pragma message "* using 128-bit vector operations in " __FILE__
#endif

// -------------------------------------------------------------------------
// use 128-bit pointer
// -------------------------------------------------------------------------
typedef long vector_op_t __attribute__((vector_size(16)));
#define EC_ISA_VECTOR_OP_WORDSIZE 16
#else
// -------------------------------------------------------------------------
// use 64-bit pointer
// -------------------------------------------------------------------------
typedef unsigned long long vector_op_t;
#define EC_ISA_VECTOR_OP_WORDSIZE 8
#endif


// -------------------------------------------------------------------------
// check if a pointer is aligend to byte_count
// -------------------------------------------------------------------------
#define is_aligned(POINTER, BYTE_COUNT) \
  (((uintptr_t)(const void *)(POINTER)) % (BYTE_COUNT) == 0)

// -------------------------------------------------------------------------
// compute byte-wise XOR of cw and dw block, ew contains the end address of cw
// -------------------------------------------------------------------------
void
byte_xor(unsigned char* cw, unsigned char* dw, unsigned char* ew);

// -------------------------------------------------------------------------
// compute word-wise XOR of cw and dw block, ew contains the end address of cw
// -------------------------------------------------------------------------
void
vector_xor(vector_op_t* cw, vector_op_t* dw, vector_op_t* ew);

// -------------------------------------------------------------------------
// compute region XOR like parity = src[0] ^ src[1] ... ^ src[src_size-]
// -------------------------------------------------------------------------
void
region_xor(unsigned char** src, unsigned char* parity, int src_size, unsigned size);

// -------------------------------------------------------------------------
// compute region XOR like parity = src[0] ^ src[1] ... ^ src[src_size-]
// using SSE2 64-byte operations
// -------------------------------------------------------------------------
void
region_sse2_xor(char** src /* array of 64-byte aligned source pointer to xor */,
                char* parity /* 64-byte aligned output pointer containing the parity */,
                int src_size /* size of the source pointer array */,
                unsigned size /* size of the region to xor */);


#endif // EC_ISA_XOR_OP_H
