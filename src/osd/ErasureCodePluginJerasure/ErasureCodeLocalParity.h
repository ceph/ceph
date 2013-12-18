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

#ifndef CEPH_ERASURE_CODE_LOCALPARITY_H
#define CEPH_ERASURE_CODE_LOCALPARITY_H

#include <set>

class ErasureCodeLocalParity {
public:

  char** ec_data;
  char** ec_coding;
  int ec_k;
  int ec_m;
  int ec_lp;
  int ec_bs;

  /**
   * @brief constructor of a local parity computation object
   * 
   * @param data pointing to (k+m) * blocksize buffer
   * @param coding pointing to (lp) * blocksize buffer
   * @param k number of data chunks
   * @param m number of ec chunks
   * @param lp number of local parities to compute
   * @param bs data block size
   */
  ErasureCodeLocalParity (char **data,
                          char **coding,
                          int k,
                          int m,
                          int lp,
                          int bs
                          ) :
  ec_data (data),
  ec_coding (coding),
  ec_k (k),
  ec_m (m),
  ec_lp (lp),
  ec_bs (bs) { }

  /**
   * @brief generate local parity chunks from input data chunks
   * 
   * This routine uses k (data**) chunks and generates lp (coding**) chunks
   */
  void generate ();

  /**
   * @brief reconstruct chunks using local parity information if possible
   * 
   * @param erasures input list of missing chunks before reco
   * @param want_to_read
   * @return list of missing chunks in erasures
   */
  int reconstruct (std::set<int> &erasures,
                   const std::set<int> &want_to_read);

  /**
   * @brief compute the index of the first data chunk for local parity computation
   * @param lpindex index of the local parity to compute
   * @return index of first data chunk
   */
  int
  rangeStart (int lpindex) {
    int n_chunks = ec_lp ? ec_k / ec_lp : 0;
    return lpindex * n_chunks;
  }

  /**
   * @brief compute the index of the last data chunk for local parity computation
   * @param lpindex index of the local parity to compute
   * @return index of last data chunk
   */
  int
  rangeStop (int lpindex) {
    int n_chunks = ec_lp ? ec_k / ec_lp : 0;
    int range = n_chunks * (lpindex + 1);
    return (range > ec_k) ? ec_k : range;
  }

  /**
   * @brief figure out which chunks are needed to read the requested chunks
   * 
   * @param want_to_read set of chunks to read
   * @param available_chunks set of chunks available
   * @param minimum set of chunks required 
   * @return 0 if decodable, otherwise -EIO
   */
  int minimum_to_decode (const std::set<int> &want_to_read,
                         const std::set<int> &available_chunks,
                         std::set<int> *minimum);

  virtual
  ~ErasureCodeLocalParity () { }
};

#endif	/* CEPH_ERASURE_CODE_LOCALPARITY_H */

