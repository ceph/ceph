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

ErasureCodeShecTableCache::~ErasureCodeShecTableCache()
{
  Mutex::Locker lock(codec_tables_guard);

  codec_technique_tables_t::const_iterator ttables_it;
  codec_tables_t::const_iterator tables_it;
  codec_tables_t_::const_iterator tables_it_;
  codec_tables_t__::const_iterator tables_it__;
  codec_table_t::const_iterator table_it;

  // clean-up all allocated tables

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
