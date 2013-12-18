// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 CERN/Switzerland
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

#include "ErasureCodeXor.h"
#include "xor.h"
#include <stdio.h>
#include "arch/intel.h"

void
ErasureCodeXor::compute (const std::set<vector_op_t*> data,
                         vector_op_t* parity,
                         int blocksize) {
  bool append = false;
  if (ceph_arch_intel_sse2) {
    vector_sse_xor(data, parity, blocksize);
  } else {
    for (std::set<vector_op_t*>::const_iterator it = data.begin();
	 it != data.end();
	 ++it, append = true) {
      
      if (append)
	vector_xor(parity, *it, blocksize / VECTOR_WORDSIZE);
      else
	vector_assign(parity, *it, blocksize / VECTOR_WORDSIZE);
    }
  }
}
