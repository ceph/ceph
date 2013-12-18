// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 CERN/Sitzerland
 *               
 *
 * Authors: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch> 
 *          
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef CEPH_VECTOROP_H
#define	CEPH_VECTOROP_H

// -------------------------------------------------------------------------
// constant used in the block alignment function to allow for vector ops
// -------------------------------------------------------------------------
#define LARGEST_VECTOR_WORDSIZE 16

// -------------------------------------------------------------------------
// switch to 128-bit XOR operations if possible 
// -------------------------------------------------------------------------
#if __GNUC__ > 4 || \
  (__GNUC__ == 4 && (__GNUC_MINOR__ >= 4) ) || \
  (__clang__ == 1 )
#pragma message "* using 128-bit vector operations in " __FILE__ 
// -------------------------------------------------------------------------
// use 128-bit pointer
// -------------------------------------------------------------------------
typedef long vector_op_t __attribute__ ((vector_size (16)));
#define VECTOR_WORDSIZE 16
#else
// -------------------------------------------------------------------------
// use 64-bit pointer
// -------------------------------------------------------------------------
typedef unsigned long long vector_op_t;
#define VECTOR_WORDSIZE 8
#endif

#endif	/* CEPH_VECTOROP_H */

