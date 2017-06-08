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

// -----------------------------------------------------------------------------
#include "ErasureCodeShecTableCache.h"
#include "ErasureCodeShec.h"
#include "common/debug.h"
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _tc_prefix(_dout)
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------

static ostream&
_tc_prefix(std::ostream* _dout) {
  return *_dout << "ErasureCodeShecTableCache: ";
}

// -----------------------------------------------------------------------------

ErasureCodeShecTableCache::~ErasureCodeShecTableCache()
{
  Mutex::Locker lock(codec_tables_guard);

  // clean-up all allocated tables
  {
    codec_technique_tables_t::const_iterator ttables_it;
    codec_tables_t::const_iterator tables_it;
    codec_tables_t_::const_iterator tables_it_;
    codec_tables_t__::const_iterator tables_it__;
    codec_table_t::const_iterator table_it;

    for (ttables_it = encoding_table.begin(); ttables_it != encoding_table.end(); ++ttables_it) {
      for (tables_it = ttables_it->second.begin(); tables_it != ttables_it->second.end(); ++tables_it) {
        for (tables_it_ = tables_it->second.begin(); tables_it_ != tables_it->second.end(); ++tables_it_) {
          for (tables_it__ = tables_it_->second.begin(); tables_it__ != tables_it_->second.end(); ++tables_it__) {
            for (table_it = tables_it__->second.begin(); table_it != tables_it__->second.end(); ++table_it) {
              if (table_it->second) {
                if (*(table_it->second)) {
                  delete *(table_it->second);
                }
                delete table_it->second;
              }
            }
          }
        }
      }
    }
  }

  {
    std::map<int, lru_map_t*>::const_iterator lru_map_it;
    std::map<int, lru_list_t*>::const_iterator lru_list_it;

    for (lru_map_it = decoding_tables.begin();
         lru_map_it != decoding_tables.end();
         ++lru_map_it) {
      if (lru_map_it->second) {
        delete lru_map_it->second;
      }
    }

    for (lru_list_it = decoding_tables_lru.begin();
         lru_list_it != decoding_tables_lru.end();
         ++lru_list_it) {
      if (lru_list_it->second) {
        delete lru_list_it->second;
      }
    }
  }
}

ErasureCodeShecTableCache::lru_map_t*
ErasureCodeShecTableCache::getDecodingTables(int technique) {
  // the caller must hold the guard mutex:
  // => Mutex::Locker lock(codec_tables_guard);

  // create an lru_map if not yet allocated
  if (!decoding_tables[technique]) {
    decoding_tables[technique] = new lru_map_t;
  }
  return decoding_tables[technique];
}

ErasureCodeShecTableCache::lru_list_t*
ErasureCodeShecTableCache::getDecodingTablesLru(int technique) {
  // the caller must hold the guard mutex:
  // => Mutex::Locker lock(codec_tables_guard);

  // create an lru_list if not yet allocated
  if (!decoding_tables_lru[technique]) {
    decoding_tables_lru[technique] = new lru_list_t;
  }
  return decoding_tables_lru[technique];
}

int**
ErasureCodeShecTableCache::getEncodingTable(int technique, int k, int m, int c, int w)
{
  Mutex::Locker lock(codec_tables_guard);
  return getEncodingTableNoLock(technique,k,m,c,w);
}

// -----------------------------------------------------------------------------

int**
ErasureCodeShecTableCache::getEncodingTableNoLock(int technique, int k, int m, int c, int w)
{
  // create a pointer to store an encoding table address
  if (!encoding_table[technique][k][m][c][w]) {
    encoding_table[technique][k][m][c][w] = new (int*);
    *encoding_table[technique][k][m][c][w] = 0;
  }
  return encoding_table[technique][k][m][c][w];
}

int*
ErasureCodeShecTableCache::setEncodingTable(int technique, int k, int m, int c, int w, int* ec_in_table)
{
  Mutex::Locker lock(codec_tables_guard);
  int** ec_out_table = getEncodingTableNoLock(technique, k, m, c, w);
  if (*ec_out_table) {
    // somebody might have deposited this table in the meanwhile, so clean
    // the input table and return the stored one
    free (ec_in_table);
    return *ec_out_table;
  } else {
    // we store the provided input table and return this one
    *encoding_table[technique][k][m][c][w] = ec_in_table;
    return ec_in_table;
  }
}

Mutex*
ErasureCodeShecTableCache::getLock()
{
  return &codec_tables_guard;
}

uint64_t
ErasureCodeShecTableCache::getDecodingCacheSignature(int k, int m, int c, int w,
                                                     int *erased, int *avails) {
  uint64_t signature = 0;
  signature = (uint64_t)k;
  signature |= ((uint64_t)m << 6);
  signature |= ((uint64_t)c << 12);
  signature |= ((uint64_t)w << 18);

  for (int i=0; i < k+m; i++) {
    signature |= ((uint64_t)(avails[i] ? 1 : 0) << (24+i));
  }
  for (int i=0; i < k+m; i++) {
    signature |= ((uint64_t)(erased[i] ? 1 : 0) << (44+i));
  }
  return signature;
}

bool
ErasureCodeShecTableCache::getDecodingTableFromCache(int* decoding_matrix,
                                                     int* dm_row,
                                                     int* dm_column,
                                                     int* minimum,
                                                     int technique,
                                                     int k,
                                                     int m,
                                                     int c,
                                                     int w,
                                                     int* erased,
                                                     int* avails) {
  // --------------------------------------------------------------------------
  // LRU decoding matrix cache
  // --------------------------------------------------------------------------

  uint64_t signature = getDecodingCacheSignature(k, m, c, w, erased, avails);
  Mutex::Locker lock(codec_tables_guard);

  dout(20) << "[ get table    ] = " << signature << dendl;

  // we try to fetch a decoding table from an LRU cache
  lru_map_t* decode_tbls_map =
    getDecodingTables(technique);

  lru_list_t* decode_tbls_lru =
    getDecodingTablesLru(technique);

  lru_map_t::iterator decode_tbls_map_it = decode_tbls_map->find(signature);
  if (decode_tbls_map_it == decode_tbls_map->end()) {
    return false;
  }

  dout(20) << "[ cached table ] = " << signature << dendl;
  // copy parameters out of the cache

  memcpy(decoding_matrix,
         decode_tbls_map_it->second.second.decoding_matrix,
         k * k * sizeof(int));
  memcpy(dm_row,
         decode_tbls_map_it->second.second.dm_row,
         k * sizeof(int));
  memcpy(dm_column,
         decode_tbls_map_it->second.second.dm_column,
         k * sizeof(int));
  memcpy(minimum,
         decode_tbls_map_it->second.second.minimum,
         (k+m) * sizeof(int));

  // find item in LRU queue and push back
  decode_tbls_lru->splice(decode_tbls_lru->end(),
                          *decode_tbls_lru,
                          decode_tbls_map_it->second.first);
  return true;
}

void
ErasureCodeShecTableCache::putDecodingTableToCache(int* decoding_matrix,
                                                   int* dm_row,
                                                   int* dm_column,
                                                   int* minimum,
                                                   int technique,
                                                   int k,
                                                   int m,
                                                   int c,
                                                   int w,
                                                   int* erased,
                                                   int* avails) {
  // --------------------------------------------------------------------------
  // LRU decoding matrix cache
  // --------------------------------------------------------------------------

  Mutex::Locker lock(codec_tables_guard);

  uint64_t signature = getDecodingCacheSignature(k, m, c, w, erased, avails);
  dout(20) << "[ put table    ] = " << signature << dendl;

  // we store a new table to the cache

  //  bufferptr cachetable;

  lru_map_t* decode_tbls_map =
    getDecodingTables(technique);

  lru_list_t* decode_tbls_lru =
    getDecodingTablesLru(technique);

  if (decode_tbls_map->count(signature)) {
    dout(20) << "[ already on table ] = " << signature << dendl;

    // find item in LRU queue and push back
    decode_tbls_lru->splice(decode_tbls_lru->end(),
                            *decode_tbls_lru,
                            (*decode_tbls_map)[signature].first);
    return;
  }

  // evt. shrink the LRU queue/map
  if ((int)decode_tbls_lru->size() >=
      ErasureCodeShecTableCache::decoding_tables_lru_length) {
    dout(20) << "[ shrink lru   ] = " << signature << dendl;
    // remove from map
    decode_tbls_map->erase(decode_tbls_lru->front());
    // remove from lru
    decode_tbls_lru->pop_front();
  }

  {
    dout(20) << "[ store table  ] = " << signature << dendl;

    decode_tbls_lru->push_back(signature);

    // allocate a new buffer
    lru_list_t::iterator it_end = decode_tbls_lru->end();
    --it_end;

    lru_entry_t &map_value =
      (*decode_tbls_map)[signature] =
      std::make_pair(it_end, DecodingCacheParameter());
    map_value.second.decoding_matrix = new int[k*k];
    map_value.second.dm_row = new int[k];
    map_value.second.dm_column = new int[k];
    map_value.second.minimum = new int[k+m];

    memcpy(map_value.second.decoding_matrix,
           decoding_matrix,
           k * k * sizeof(int));
    memcpy(map_value.second.dm_row,
           dm_row,
           k * sizeof(int));
    memcpy(map_value.second.dm_column,
           dm_column,
           k * sizeof(int));
    memcpy(map_value.second.minimum,
           minimum,
           (k+m) * sizeof(int));

    dout(20) << "[ cache size   ] = " << decode_tbls_lru->size() << dendl;
  }
}
