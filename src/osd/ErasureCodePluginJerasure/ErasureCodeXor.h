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

#ifndef CEPH_ERASURE_CODE_XOR_H
#define CEPH_ERASURE_CODE_XOR_H

#include "vectorop.h"
#include <set>

class ErasureCodeXor {
public:

  /**
   * @brief compute (=xor) all given data blocks and store the encoded block
   * @param data set of addresses to data blocks to xor
   * @param encoded address to store the xor'ed result
   * @param blocksize size of each data block
   * 
   * The caller has to make supre, that the size of the vector data block is
   * in agreement with the blocksize given.
   */
  void compute (const std::set<vector_op_t*> data,
                vector_op_t* parity,
                int blocksize);

  ErasureCodeXor () { }

  virtual
  ~ErasureCodeXor () { }
};

#endif	/* CEPH_ERASURE_CODE_XOR_H */

