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
#include "common/ceph_mutex.h"
#include "erasure-code/ErasureCodeInterface.h"
// -----------------------------------------------------------------------------
#include <list>
// -----------------------------------------------------------------------------

class ErasureCodeShecTableCache {
  // ---------------------------------------------------------------------------
  // This class implements a table cache for encoding and decoding matrices.
  // Encoding matrices are shared for the same (k,m,c,w) combination.
  // It supplies a decoding matrix lru cache which is shared for identical
  // matrix types e.g. there is one cache (lru-list + lru-map)
  // ---------------------------------------------------------------------------

  class DecodingCacheParameter {
   public:
    int* decoding_matrix;  // size: k*k
    int* dm_row;  // size: k
    int* dm_column;  // size: k
    int* minimum;  // size: k+m
    DecodingCacheParameter() {
      decoding_matrix = nullptr;
      dm_row = nullptr;
      dm_column = nullptr;
      minimum = nullptr;
    }
    ~DecodingCacheParameter() {
      if (decoding_matrix) {
        delete[] decoding_matrix;
      }
      if (dm_row) {
        delete[] dm_row;
      }
      if (dm_column) {
        delete[] dm_column;
      }
      if (minimum) {
        delete[] minimum;
      }
    }
  };

 public:

  static const int decoding_tables_lru_length = 10000;
  typedef std::pair<std::list<uint64_t>::iterator,
                    DecodingCacheParameter> lru_entry_t;
  typedef std::map< int, int** > codec_table_t;
  typedef std::map< int, codec_table_t > codec_tables_t__;
  typedef std::map< int, codec_tables_t__ > codec_tables_t_;
  typedef std::map< int, codec_tables_t_ > codec_tables_t;
  typedef std::map< int, codec_tables_t > codec_technique_tables_t;
  // int** matrix = codec_technique_tables_t[technique][k][m][c][w]
  
  typedef std::map< uint64_t, lru_entry_t > lru_map_t;
  typedef std::list< uint64_t > lru_list_t;

  ErasureCodeShecTableCache()  = default;
  virtual ~ErasureCodeShecTableCache();
  // mutex used to protect modifications in encoding/decoding table maps
  ceph::mutex codec_tables_guard = ceph::make_mutex("shec-lru-cache");
  
  bool getDecodingTableFromCache(int* matrix,
                                 int* dm_row, int* dm_column,
                                 int* minimum,
                                 int technique,
                                 int k, int m, int c, int w,
                                 int* want, int* avails);

  void putDecodingTableToCache(int* matrix,
                               int* dm_row, int* dm_column,
                               int* minimum,
                               int technique,
                               int k, int m, int c, int w,
                               int* want, int* avails);

  int** getEncodingTable(int technique, int k, int m, int c, int w);
  int** getEncodingTableNoLock(int technique, int k, int m, int c, int w);
  int* setEncodingTable(int technique, int k, int m, int c, int w, int*);
  
 private:
  // encoding table accessed via table[matrix][k][m][c][w]
  // decoding table cache accessed via map[matrixtype]
  // decoding table lru list accessed via list[matrixtype]
  codec_technique_tables_t encoding_table;
  std::map<int, lru_map_t*> decoding_tables;
  std::map<int, lru_list_t*> decoding_tables_lru;

  lru_map_t* getDecodingTables(int technique);
  lru_list_t* getDecodingTablesLru(int technique);
  uint64_t getDecodingCacheSignature(int k, int m, int c, int w,
                                     int *want, int *avails);

  ceph::mutex* getLock();
};

#endif
