// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 CERN/Switzerland
 *
 * Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef CEPH_ERASURE_CODE_XOR_H
#define CEPH_ERASURE_CODE_XOR_H

// -----------------------------------------------------------------------------

#include "osd/ErasureCode.h"
#include "osd/ErasureCodePluginJerasure/vectorop.h"

// -----------------------------------------------------------------------------

/*! @file ErasureCodeXor.h
    @brief EC Plugin implementing a RAID4-like simple parity based algorithm.
  
   The class has an optimization to use SSE2 assembler to do fast region
   xor'ing if supported by the platform. 
   If not available it falls back to vector operations.  
 */


// -----------------------------------------------------------------------------

class ErasureCodeXor : public ErasureCode {
public:
  static const int DEFAULT_K = 7;

  int k;

  ErasureCodeXor () { }

  virtual
  ~ErasureCodeXor () { }

  virtual unsigned int
  get_chunk_count () const {
    return k + 1;
  }

  virtual unsigned int
  get_data_chunk_count () const {
    return k;
  }

  virtual unsigned int get_chunk_size (unsigned int object_size) const;

  virtual int minimum_to_decode (const std::set<int> &want_to_read,
                                 const std::set<int> &available_chunks,
                                 std::set<int> *minimum);

  virtual int minimum_to_decode_with_cost (const std::set<int> &want_to_read,
                                           const std::map<int, int> &available,
                                           std::set<int> *minimum);

  virtual int encode_chunks (vector<bufferlist> &chunks);

  virtual int decode_chunks (vector<bool> erasures,
                             vector<bufferlist> &chunks);

  virtual unsigned int get_alignment () const;

  void init (const std::map<std::string, std::string> &parameters);

  virtual void parse (const map<std::string, std::string> &parameters);

  static int to_int (const std::string &name,
                     const map<std::string, std::string> &parameters,
                     int default_value);

  /**
   * @brief function computing region xor like p=a^b^c
   * 
   * This function requires aligned data and parity pointer and an aligned
   * blocksize. If SSE2 is available the alignment must fit 64 bytes.
   * If vector operations are avaialble 16 bytes, otherwise 8 bytes.
   * 
   * @param data set with pointer to region chunks to xor
   * @param parity pointer to buffer to store result of xor
   * @param blocksize size of one chunk
   * 
   */
  void compute (const std::set<vector_op_t*> data,
                vector_op_t* parity,
                int blocksize);
};

#endif
