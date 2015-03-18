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
#include <stdint.h>
#include <list>
#include <string>
#include <map>
#include <utility>
#include "common/Mutex.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "common/debug.h"
// -----------------------------------------------------------------------------

class ErasureCodeShecTableCache {
  class DecodingCacheParameter {
   public:
    int* decoding_matrix;  // size: k*k
    int* dm_ids;  // size: k
    int* minimum;  // size: k+m
    DecodingCacheParameter() {
      decoding_matrix = 0;
      dm_ids = 0;
      minimum = 0;
    }
    ~DecodingCacheParameter() {
      if (decoding_matrix) {
        delete[] decoding_matrix;
      }
      if (dm_ids) {
        delete[] dm_ids;
      }
      if (minimum) {
        delete[] minimum;
      }
    }
  };

  // ---------------------------------------------------------------------------
  // This class implements a table cache for encoding and decoding matrices.
  // Encoding matrices are shared for the same (k,m,c,w) combination.
  // It supplies a decoding matrix lru cache which is shared for identical
  // matrix types e.g. there is one cache (lru-list + lru-map)
  // ---------------------------------------------------------------------------

 public:
  static const int decoding_tables_lru_length = 2516;

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

  ErasureCodeShecTableCache() :
    codec_tables_guard("shec-lru-cache")
  {}

  virtual ~ErasureCodeShecTableCache();

  // mutex used to protect modifications in encoding/decoding table maps
  Mutex codec_tables_guard;

  bool getDecodingTableFromCache(int* matrix,
                                 int* dm_ids,
                                 int* minimum,
                                 int technique,
                                 int k,
                                 int m,
                                 int c,
                                 int w,
                                 int* erased,
                                 int* avails);

  void putDecodingTableToCache(int* matrix,
                               int* dm_ids,
                               int* minimum,
                               int technique,
                               int k,
                               int m,
                               int c,
                               int w,
                               int* erased,
                               int* avails);

  int** getEncodingTable(int technique, int k, int m, int c, int w);
  int** getEncodingTableNoLock(int technique, int k, int m, int c, int w);
  int* setEncodingTable(int technique, int k, int m, int c, int w, int*);

  int getDecodingTableCacheSize(int technique);

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
                                     int *erased, int *avails);

  Mutex* getLock();
};

#endif
