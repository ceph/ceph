// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 FUJITSU LIMITED
 * Copyright (C) 2014 CERN (Switzerland)
 *
 * Author: Takanori Nakao <nakao.takanori@jp.fujitsu.com>
 * Author: Takeshi Miyamae <miyamae.takeshi@jp.fujitsu.com>
 * Author: Andreas-Joachim Peters <Andreas.Joachim.Peters@cern.ch>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_ERASURE_CODE_SHEC_TABLE_CACHE_H
#define CEPH_ERASURE_CODE_SHEC_TABLE_CACHE_H

// -----------------------------------------------------------------------------
#include "common/Mutex.h"
#include "erasure-code/ErasureCodeInterface.h"
// -----------------------------------------------------------------------------
#include <list>
// -----------------------------------------------------------------------------

class ErasureCodeShecTableCache {
  // ---------------------------------------------------------------------------
  // This class implements a table cache for encoding and decoding matrices.
  // Encoding matrices are shared for the same (k,m,c,w) combination.
  // ---------------------------------------------------------------------------

 public:

  typedef std::pair<std::list<std::string>::iterator, bufferptr> lru_entry_t;
  typedef std::map< int, int** > codec_table_t;
  typedef std::map< int, codec_table_t > codec_tables_t__;
  typedef std::map< int, codec_tables_t__ > codec_tables_t_;
  typedef std::map< int, codec_tables_t_ > codec_tables_t;
  typedef std::map< int, codec_tables_t > codec_technique_tables_t;
  // int** matrix = codec_technique_tables_t[technique][k][m][c][w]
  
 ErasureCodeShecTableCache() :
  codec_tables_guard("shec-lru-cache")
    {
    }
  
  virtual ~ErasureCodeShecTableCache();
  
  Mutex codec_tables_guard; // mutex used to protect modifications in encoding/decoding table maps
  
  int** getEncodingTable(int technique, int k, int m, int c, int w);
  int** getEncodingTableNoLock(int technique, int k, int m, int c, int w);
  int* setEncodingTable(int technique, int k, int m, int c, int w, int*);
  
 private:
  codec_technique_tables_t encoding_table; // encoding coefficients accessed via table[technique][k][m]
  
  Mutex* getLock();
  
};

#endif
