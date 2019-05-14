/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CERN (Switzerland)
 *
 * Author: Andreas-Joachim Peters <Andreas.Joachim.Peters@cern.ch>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

/**
 * @file   ErasureCodeIsaTableCache.h
 *
 * @brief  Erasure Code Isa CODEC Table Cache
 *
 * The INTEL ISA-L library supports two pre-defined encoding matrices (cauchy = default, reed_sol_van = default)
 * The default CODEC implementation using these two matrices is implemented in class ErasureCodeIsaDefault.
 * ISA-L allows to use custom matrices which might be added later as implementations deriving from the base class ErasoreCodeIsa.
 */

#ifndef CEPH_ERASURE_CODE_ISA_TABLE_CACHE_H
#define CEPH_ERASURE_CODE_ISA_TABLE_CACHE_H

// -----------------------------------------------------------------------------
#include "common/Mutex.h"
#include "erasure-code/ErasureCodeInterface.h"
// -----------------------------------------------------------------------------
#include <list>
// -----------------------------------------------------------------------------

class ErasureCodeIsaTableCache {
  // ---------------------------------------------------------------------------
  // This class implements a table cache for encoding and decoding matrices.
  // Encoding matrices are shared for the same (k,m) combination. It supplies
  // a decoding matrix lru cache which is shared for identical
  // matrix types e.g. there is one cache (lru-list + lru-map) for Cauchy and
  // one for Vandermonde matrices!
  // ---------------------------------------------------------------------------

public:

  // the cache size is sufficient up to (12,4) decodings

  static const int decoding_tables_lru_length = 2516;

  typedef std::pair<std::list<std::string>::iterator, ceph::buffer::ptr> lru_entry_t;
  typedef std::map< int, unsigned char** > codec_table_t;
  typedef std::map< int, codec_table_t > codec_tables_t;
  typedef std::map< int, codec_tables_t > codec_technique_tables_t;

  typedef std::map< std::string, lru_entry_t > lru_map_t;
  typedef std::list< std::string > lru_list_t;

  ErasureCodeIsaTableCache() :
  codec_tables_guard("isa-lru-cache")
  {
  }

  virtual ~ErasureCodeIsaTableCache();

  Mutex codec_tables_guard; // mutex used to protect modifications in encoding/decoding table maps

  bool getDecodingTableFromCache(std::string &signature,
                                 unsigned char* &table,
                                 int matrixtype,
                                 int k,
                                 int m);

  void putDecodingTableToCache(std::string&,
                               unsigned char*&,
                               int matrixtype,
                               int k,
                               int m);

  unsigned char** getEncodingTable(int matrix, int k, int m);
  unsigned char** getEncodingCoefficient(int matrix, int k, int m);

  unsigned char** getEncodingTableNoLock(int matrix, int k, int m);
  unsigned char** getEncodingCoefficientNoLock(int matrix, int k, int m);

  unsigned char* setEncodingTable(int matrix, int k, int m, unsigned char*);
  unsigned char* setEncodingCoefficient(int matrix, int k, int m, unsigned char*);

  int getDecodingTableCacheSize(int matrixtype = 0);

private:
  codec_technique_tables_t encoding_coefficient; // encoding coefficients accessed via table[matrix][k][m]
  codec_technique_tables_t encoding_table; // encoding coefficients accessed via table[matrix][k][m]

  std::map<int, lru_map_t*> decoding_tables; // decoding table cache accessed via map[matrixtype]
  std::map<int, lru_list_t*> decoding_tables_lru; // decoding table lru list accessed via list[matrixtype]

  lru_map_t* getDecodingTables(int matrix_type);

  lru_list_t* getDecodingTablesLru(int matrix_type);

  Mutex* getLock();

};

#endif
