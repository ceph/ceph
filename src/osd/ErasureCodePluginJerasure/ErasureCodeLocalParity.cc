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

#include "ErasureCodeLocalParity.h"
#include "ErasureCodeXor.h"
#include <errno.h>
#include <stdio.h>

void
ErasureCodeLocalParity::generate () {
  // ---------------------------------------------------------------------------
  // place the vector operation pointer
  // ---------------------------------------------------------------------------
  vector_op_t * vop_data[ec_k];
  vector_op_t * vop_coding[ec_lp];

  for (int i = 0; i < ec_k; i++) {
    vop_data[i] = (vector_op_t *) ec_data[i];
  }

  for (int i = 0; i < ec_lp; i++) {
    vop_coding[i] = (vector_op_t *) ec_coding[i];
  }

  // ---------------------------------------------------------------------------
  // compute local parities : basic pyramid code
  // ---------------------------------------------------------------------------
  for (int i_lp = 0; i_lp < ec_lp; i_lp++) {
    // -------------------------------------------------------------------------
    // configure the set of local buffers to xor
    // -------------------------------------------------------------------------
    std::set<vector_op_t*> buffer_set;

    for (int l = rangeStart(i_lp); l < rangeStop(i_lp); l++) {
      buffer_set.insert(vop_data[l]);
    }

    // -------------------------------------------------------------------------
    // compute a local parity
    // -------------------------------------------------------------------------
    ErasureCodeXor ecXor;
    ecXor.compute(buffer_set, vop_coding[i_lp], ec_bs);
  }
}

int
ErasureCodeLocalParity::reconstruct (
                                     std::set<int> &erasures,
                                     const std::set<int> &want_to_read) {
  vector_op_t * dataword[ec_k];
  vector_op_t * codingword[ec_lp];

  bool all_recovered = true;

  // data chunks
  for (int i = 0; i < ec_k; i++) {
    dataword[i] = (vector_op_t*) ec_data[i];
  }

  // parity chunks
  for (int i = 0; i < ec_lp; i++) {
    codingword[i] = (vector_op_t*) ec_coding[ec_m + i];
  }

  // ---------------------------------------------------------------------------
  // decode using a local parity : basic pyramid code
  // ---------------------------------------------------------------------------
  for (int i_lp = 0; i_lp < ec_lp; i_lp++) {
    // see how many are missing here
    int n_miss = 0;
    int reco_chunk = 0;
    std::set<int>reco_erasures;

    for (int l = rangeStart(i_lp); l < rangeStop(i_lp); l++) {
      if (erasures.count(l)) {
        n_miss++;
        reco_chunk = l;
      }
      else {
        reco_erasures.insert(l);
      }
    }

    if (n_miss == 1) {
      // -----------------------------------------------------------------------
      // only reconstruct if this chunk is actually wanted
      // -> we can repair this with local parity
      // -----------------------------------------------------------------------
      // -----------------------------------------------------------------------
      // XOR all data blocks existing
      // -----------------------------------------------------------------------
      std::set<vector_op_t*> reco_chunks;
      for (std::set<int>::iterator r = reco_erasures.begin();
        r != reco_erasures.end();
        ++r) {
        reco_chunks.insert((vector_op_t*) dataword[*r]);
      }
      // -----------------------------------------------------------------------
      // add the parity chunk
      // -----------------------------------------------------------------------
      reco_chunks.insert((vector_op_t*) codingword[i_lp]);

      ErasureCodeXor ecXor;
      ecXor.compute(reco_chunks, dataword[reco_chunk], ec_bs);

      erasures.erase(reco_chunk);
    }
  }
  // ---------------------------------------------------------------------------
  // check if everything in want_to_read has been reconstructed 
  // e.g. nothing is left in the remaining_erasures set
  // --------------------------------------------------------------------------
  for (std::set<int>::iterator it = want_to_read.begin();
    it != want_to_read.end();
    it++) {
    if (erasures.count(*it)) {
      all_recovered = false;
      break;
    }
  }
  return all_recovered;
}

int
ErasureCodeLocalParity::minimum_to_decode (const std::set<int>& want_to_read,
                                           const std::set<int>& available_chunks,
                                           std::set<int>* minimum) {
  int lp_recovered = 0;
  std::set<int>::const_iterator i;
  for (i = want_to_read.begin(); i != want_to_read.end(); ++i) {
    if (available_chunks.count(*i)) {
      // ---------------------------------------------------------------------
      // if wanted and ok, just add it to the minimum set
      // ---------------------------------------------------------------------
      minimum->insert(*i);
    }
    else {
      // ---------------------------------------------------------------------
      // check if only one chunk is missing in a local parity subset
      // ---------------------------------------------------------------------
      ErasureCodeLocalParity ecParity(0, 0, ec_k, ec_m, ec_lp, 0);

      for (int s = 0; s < ec_lp; s++) {
        // -------------------------------------------------------------------
        // check if <i> is in the local parity subgroup
        // -------------------------------------------------------------------
        if ((*i >= ecParity.rangeStart(s)) && (*i < ecParity.rangeStop(s))) {
          // -----------------------------------------------------------------
          // this is our subgroup
          // -----------------------------------------------------------------
          size_t n_miss = 0;
          // -----------------------------------------------------------------
          // count the missing chunks
          // -----------------------------------------------------------------
          for (int l = ecParity.rangeStart(s); l < ecParity.rangeStop(s); l++) {
            if (!available_chunks.count(l)) {
              n_miss++;
            }
          }
          // -----------------------------------------------------------------
          // if only one is missing, we recover it with the subgroup
          // -----------------------------------------------------------------
          if (n_miss == 1) {
            for (int l = ecParity.rangeStart(s); l < ecParity.rangeStop(s); l++) {
              if (available_chunks.count(l)) {
                minimum->insert(l);
              }
            }
            minimum->insert(ec_k + ec_m + s);
            lp_recovered++;
          }
          else {
            // ---------------------------------------------------------------
            // if there are more missing we need to have at least k of (k+m) 
            // chunks
            // ---------------------------------------------------------------
            unsigned j;
            std::set<int>::const_iterator o;

            for (o = available_chunks.begin(), j = 0;
              j < (unsigned) (ec_k - lp_recovered);
              o++, j++) {
              if ((*o) >= (ec_k + ec_m)) {
                // -----------------------------------------------------------
                // local parity is not usable for erasure recovery
                // -----------------------------------------------------------
                return -EIO;
              }
              minimum->insert(*o);
            }
          }
        }
      }
    }
  }
  return 0;
}
