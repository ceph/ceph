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

class ErasureCodeIsaTableCache  {
  // ---------------------------------------------------------------------------
  // This class implements a table caches for encoding and decoding matrixes.
  // Encoding matrixes are shared for the same (k,m) combination. It supplies
  // a decoding matrix lru cache which is shared for identical
  // matrix types e.g. there is one cache (lru-list + lru-map) for Cauchy and 
  // one for Vandermonde matrices!
  // ---------------------------------------------------------------------------

public:
  
  // the cache size is sufficient up to (12,4) decoding's

  static const int decoding_tables_lru_length=2516;  

  typedef std::pair<std::list<std::string>::iterator, bufferptr> lru_entry_t;
  typedef std::map< int, unsigned char** > codec_table_t; 
  typedef std::map< int , codec_table_t > codec_tables_t; 
  typedef std::map< int, codec_tables_t > codec_technique_tables_t; 

  typedef std::map< std::string, lru_entry_t > lru_map_t;
  typedef std::list< std::string > lru_list_t;


 ErasureCodeIsaTableCache() :
  codec_tables_guard("isa-lru-cache") {}

  virtual ~ErasureCodeIsaTableCache() {
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

  int getDecodingCacheSize( int matrixtype )
  {
    Mutex::Locker lock(codec_tables_guard);
    if (decoding_tables[matrixtype])
      return decoding_tables[matrixtype]->size();
    else
      return -1;
  }

  lru_map_t* getDecodingTables( int matrix_type ) {
    Mutex::Locker lock(codec_tables_guard);
    // create an lru_map if not yet allocated
    if (!decoding_tables[matrix_type]) {
      decoding_tables[matrix_type] = new lru_map_t;
    }
    return decoding_tables[matrix_type];
  }
  
  lru_list_t* getDecodingTablesLru( int matrix_type ) {
    Mutex::Locker lock(codec_tables_guard);
    // create an lru_list if not yet allocated
    if (!decoding_tables_lru[matrix_type]) {
      decoding_tables_lru[matrix_type] = new lru_list_t;
    }
    return decoding_tables_lru[matrix_type];
  }

  unsigned char** getEncodingTable(int matrix, int k, int m)
  {
    Mutex::Locker lock(codec_tables_guard);
    // create a pointer to store an encoding table address
    if (! encoding_table[matrix][k][m] ) { 
      encoding_table[matrix][k][m] = new (unsigned char*);
      *encoding_table[matrix][k][m] = 0;
    }
    return encoding_table[matrix][k][m];
  }

  unsigned char** getEncodingCoefficient(int matrix, int k, int m)
  {
    Mutex::Locker lock(codec_tables_guard);
    // create a pointer to store an encoding coefficients adddress
    if (! encoding_coefficient[matrix][k][m] ) {
      encoding_coefficient[matrix][k][m] = new (unsigned char*);
      *encoding_coefficient[matrix][k][m] = 0;
    }
    return encoding_coefficient[matrix][k][m];
  }

  Mutex* getLock() {return &codec_tables_guard;}

 private:
  Mutex codec_tables_guard; // mutex used to protect modifications in decoding table maps

  codec_technique_tables_t encoding_coefficient;  // encoding coefficients accessed via table[matrix][k][m]
  codec_technique_tables_t encoding_table;        // encoding coefficeints accessed via table[matrix][k][m]

  std::map<int, lru_map_t*>  decoding_tables;          // decoding table cache accessed via map[matrixtype]
  std::map<int, lru_list_t*> decoding_tables_lru;      // decoding table lru list accessed via list[matrixtype]
};

#endif
