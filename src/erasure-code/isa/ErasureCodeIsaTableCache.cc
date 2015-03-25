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
 * @file   ErasureCodeIsaTableCache.cc
 *
 * @brief  Erasure Code Isa CODEC Table Cache
 *
 * The INTEL ISA-L library supports two pre-defined encoding matrices (cauchy = default, reed_sol_van = default)
 * The default CODEC implementation using these two matrices is implemented in class ErasureCodeIsaDefault.
 * ISA-L allows to use custom matrices which might be added later as implementations deriving from the base class ErasoreCodeIsa.
 */

// -----------------------------------------------------------------------------
#include "ErasureCodeIsaTableCache.h"
#include "ErasureCodeIsa.h"
#include "common/debug.h"
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _tc_prefix(_dout)
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------

static ostream&
_tc_prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodeIsaTableCache: ";
}

// -----------------------------------------------------------------------------

ErasureCodeIsaTableCache::~ErasureCodeIsaTableCache()
{
  Mutex::Locker lock(codec_tables_guard);

  codec_technique_tables_t::const_iterator ttables_it;
  codec_tables_t::const_iterator tables_it;
  codec_table_t::const_iterator table_it;

  std::map<int, lru_map_t*>::const_iterator lru_map_it;
  std::map<int, lru_list_t*>::const_iterator lru_list_it;

  // clean-up all allocated tables
  for (ttables_it = encoding_coefficient.begin(); ttables_it != encoding_coefficient.end(); ++ttables_it) {
    for (tables_it = ttables_it->second.begin(); tables_it != ttables_it->second.end(); ++tables_it) {
      for (table_it = tables_it->second.begin(); table_it != tables_it->second.end(); ++table_it) {
        if (table_it->second) {
          if (*(table_it->second)) {
            delete *(table_it->second);
          }
          delete table_it->second;
        }
      }
    }
  }

  for (ttables_it = encoding_table.begin(); ttables_it != encoding_table.end(); ++ttables_it) {
    for (tables_it = ttables_it->second.begin(); tables_it != ttables_it->second.end(); ++tables_it) {
      for (table_it = tables_it->second.begin(); table_it != tables_it->second.end(); ++table_it) {
        if (table_it->second) {
          if (*(table_it->second)) {
            delete *(table_it->second);
          }
          delete table_it->second;
        }
      }
    }
  }

  for (lru_map_it = decoding_tables.begin(); lru_map_it != decoding_tables.end(); ++lru_map_it) {
    if (lru_map_it->second) {
      delete lru_map_it->second;
    }
  }

  for (lru_list_it = decoding_tables_lru.begin(); lru_list_it != decoding_tables_lru.end(); ++lru_list_it) {
    if (lru_list_it->second) {
      delete lru_list_it->second;
    }
  }
}

// -----------------------------------------------------------------------------

int
ErasureCodeIsaTableCache::getDecodingTableCacheSize(int matrixtype)
{
  Mutex::Locker lock(codec_tables_guard);
  if (decoding_tables[matrixtype])
    return decoding_tables[matrixtype]->size();
  else
    return -1;
}

// -----------------------------------------------------------------------------

ErasureCodeIsaTableCache::lru_map_t*
ErasureCodeIsaTableCache::getDecodingTables(int matrix_type)
{
  // the caller must hold the guard mutex:
  // => Mutex::Locker lock(codec_tables_guard);

  // create an lru_map if not yet allocated
  if (!decoding_tables[matrix_type]) {
    decoding_tables[matrix_type] = new lru_map_t;
  }
  return decoding_tables[matrix_type];
}

// -----------------------------------------------------------------------------

ErasureCodeIsaTableCache::lru_list_t*
ErasureCodeIsaTableCache::getDecodingTablesLru(int matrix_type)
{
  // the caller must hold the guard mutex:
  // => Mutex::Locker lock(codec_tables_guard);

  // create an lru_list if not yet allocated
  if (!decoding_tables_lru[matrix_type]) {
    decoding_tables_lru[matrix_type] = new lru_list_t;
  }
  return decoding_tables_lru[matrix_type];
}

// -----------------------------------------------------------------------------

unsigned char**
ErasureCodeIsaTableCache::getEncodingTable(int matrix, int k, int m)
{
  Mutex::Locker lock(codec_tables_guard);
  return getEncodingTableNoLock(matrix,k,m);
}

// -----------------------------------------------------------------------------

unsigned char**
ErasureCodeIsaTableCache::getEncodingTableNoLock(int matrix, int k, int m)
{
  // create a pointer to store an encoding table address
  if (!encoding_table[matrix][k][m]) {
    encoding_table[matrix][k][m] = new (unsigned char*);
    *encoding_table[matrix][k][m] = 0;
  }
  return encoding_table[matrix][k][m];
}

// -----------------------------------------------------------------------------

unsigned char**
ErasureCodeIsaTableCache::getEncodingCoefficient(int matrix, int k, int m)
{
  Mutex::Locker lock(codec_tables_guard);
  return getEncodingCoefficientNoLock(matrix,k,m);
}

// -----------------------------------------------------------------------------

unsigned char**
ErasureCodeIsaTableCache::getEncodingCoefficientNoLock(int matrix, int k, int m)
{
  // create a pointer to store an encoding coefficients adddress
  if (!encoding_coefficient[matrix][k][m]) {
    encoding_coefficient[matrix][k][m] = new (unsigned char*);
    *encoding_coefficient[matrix][k][m] = 0;
  }
  return encoding_coefficient[matrix][k][m];
}

// -----------------------------------------------------------------------------

unsigned char*
ErasureCodeIsaTableCache::setEncodingTable(int matrix, int k, int m, unsigned char* ec_in_table)
{
  Mutex::Locker lock(codec_tables_guard);
  unsigned char** ec_out_table = getEncodingTableNoLock(matrix, k, m);
  if (*ec_out_table) {
    // somebody might have deposited this table in the meanwhile, so clean
    // the input table and return the stored one
    free (ec_in_table);
    return *ec_out_table;
  } else {
    // we store the provided input table and return this one
    *encoding_table[matrix][k][m] = ec_in_table;
    return ec_in_table;
  }
}

// -----------------------------------------------------------------------------

unsigned char*
ErasureCodeIsaTableCache::setEncodingCoefficient(int matrix, int k, int m, unsigned char* ec_in_coeff)
{
  Mutex::Locker lock(codec_tables_guard);
  unsigned char** ec_out_coeff = getEncodingCoefficientNoLock(matrix, k, m);
  if (*ec_out_coeff) {
    // somebody might have deposited these coefficients in the meanwhile, so clean
    // the input coefficients and return the stored ones
    free (ec_in_coeff);
    return *ec_out_coeff;
  } else {
    // we store the provided input coefficients and return these
    *encoding_coefficient[matrix][k][m] = ec_in_coeff;
    return ec_in_coeff;
  }
}

// -----------------------------------------------------------------------------

Mutex*
ErasureCodeIsaTableCache::getLock()
{
  return &codec_tables_guard;
}

// -----------------------------------------------------------------------------

bool
ErasureCodeIsaTableCache::getDecodingTableFromCache(std::string &signature,
                                                    unsigned char* &table,
                                                    int matrixtype,
                                                    int k,
                                                    int m)
{
  // --------------------------------------------------------------------------
  // LRU decoding matrix cache
  // --------------------------------------------------------------------------

  dout(12) << "[ get table    ] = " << signature << dendl;

  // we try to fetch a decoding table from an LRU cache
  bool found = false;

  Mutex::Locker lock(codec_tables_guard);

  lru_map_t* decode_tbls_map =
    getDecodingTables(matrixtype);

  lru_list_t* decode_tbls_lru =
    getDecodingTablesLru(matrixtype);

  if (decode_tbls_map->count(signature)) {
    dout(12) << "[ cached table ] = " << signature << dendl;
    // copy the table out of the cache
    memcpy(table, (*decode_tbls_map)[signature].second.c_str(), k * (m + k)*32);
    // find item in LRU queue and push back
    dout(12) << "[ cache size   ] = " << decode_tbls_lru->size() << dendl;
    decode_tbls_lru->splice( (decode_tbls_lru->begin()), *decode_tbls_lru, (*decode_tbls_map)[signature].first);
    found = true;
  }

  return found;
}

// -----------------------------------------------------------------------------

void
ErasureCodeIsaTableCache::putDecodingTableToCache(std::string &signature,
                                                  unsigned char* &table,
                                                  int matrixtype,
                                                  int k,
                                                  int m)
{
  // --------------------------------------------------------------------------
  // LRU decoding matrix cache
  // --------------------------------------------------------------------------

  dout(12) << "[ put table    ] = " << signature << dendl;

  // we store a new table to the cache

  bufferptr cachetable;

  Mutex::Locker lock(codec_tables_guard);

  lru_map_t* decode_tbls_map =
    getDecodingTables(matrixtype);

  lru_list_t* decode_tbls_lru =
    getDecodingTablesLru(matrixtype);

  // evt. shrink the LRU queue/map
  if ((int) decode_tbls_lru->size() >= ErasureCodeIsaTableCache::decoding_tables_lru_length) {
    dout(12) << "[ shrink lru   ] = " << signature << dendl;
    // reuse old buffer
    cachetable = (*decode_tbls_map)[decode_tbls_lru->back()].second;

    if ((int) cachetable.length() != (k * (m + k)*32)) {
      // we need to replace this with a different size buffer
      cachetable = buffer::create(k * (m + k)*32);
    }

    // remove from map
    decode_tbls_map->erase(decode_tbls_lru->back());
    // remove from lru
    decode_tbls_lru->pop_back();
    // add to the head of lru
    decode_tbls_lru->push_front(signature);
    // add the new to the map
    (*decode_tbls_map)[signature] = std::make_pair(decode_tbls_lru->begin(), cachetable);
  } else {
    dout(12) << "[ store table  ] = " << signature << dendl;
    // allocate a new buffer
    cachetable = buffer::create(k * (m + k)*32);
    decode_tbls_lru->push_front(signature);
    (*decode_tbls_map)[signature] = std::make_pair(decode_tbls_lru->begin(), cachetable);
    dout(12) << "[ cache size   ] = " << decode_tbls_lru->size() << dendl;
  }

  // copy-in the new table
  memcpy(cachetable.c_str(), table, k * (m + k)*32);
}
