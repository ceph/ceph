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
 * @file   ErasureCodeIsa.cc
 *
 * @brief  Erasure Code CODEC using the INTEL ISA-L library.
 *
 * The INTEL ISA-L library supports two pre-defined encoding matrices (cauchy = default, reed_sol_van = default)
 * The default CODEC implementation using these two matrices is implemented in class ErasureCodeIsaDefault.
 * ISA-L allows to use custom matrices which might be added later as implementations deriving from the base class ErasoreCodeIsa.
 */

#ifndef CEPH_ERASURE_CODE_ISA_L_H
#define CEPH_ERASURE_CODE_ISA_L_H

// -----------------------------------------------------------------------------
#include "common/Mutex.h"
#include "erasure-code/ErasureCodeInterface.h"
// -----------------------------------------------------------------------------
#include <list>
// -----------------------------------------------------------------------------

class ErasureCodeIsa : public ErasureCodeInterface {
public:
  enum eMatrix {kVandermonde=0, kCauchy=1};

  int k;
  int m;
  int w;
  const char *technique;
  string ruleset_root;
  string ruleset_failure_domain;

  ErasureCodeIsa(const char *_technique) :
  technique(_technique),
  ruleset_root("default"),
  ruleset_failure_domain("host")
  {
  }

  virtual
  ~ErasureCodeIsa()
  {
  }

  virtual int create_ruleset(const string &name,
                             CrushWrapper &crush,
                             ostream *ss) const;

  virtual unsigned int
  get_chunk_count() const
  {
    return k + m;
  }

  virtual unsigned int
  get_data_chunk_count() const
  {
    return k;
  }

  virtual unsigned int get_chunk_size(unsigned int object_size) const;

  virtual int minimum_to_decode(const set<int> &want_to_read,
                                const set<int> &available_chunks,
                                set<int> *minimum);

  virtual int minimum_to_decode_with_cost(const set<int> &want_to_read,
                                          const map<int, int> &available,
                                          set<int> *minimum);

  virtual int encode(const set<int> &want_to_encode,
                     const bufferlist &in,
                     map<int, bufferlist> *encoded);

  virtual int decode(const set<int> &want_to_read,
                     const map<int, bufferlist> &chunks,
                     map<int, bufferlist> *decoded);

  void init(const map<std::string, std::string> &parameters);

  virtual void isa_encode(char **data,
                          char **coding,
                          int blocksize) = 0;


  virtual int isa_decode(int *erasures,
                         char **data,
                         char **coding,
                         int blocksize) = 0;

  virtual unsigned get_alignment() const = 0;

  virtual void parse(const map<std::string, std::string> &parameters) = 0;

  virtual void prepare() = 0;

  static int to_int(const std::string &name,
                    const map<std::string, std::string> &parameters,
                    int default_value);
};

// -----------------------------------------------------------------------------

class ErasureCodeIsaDefault : public ErasureCodeIsa {

  int matrixtype;

public:

  static const int DEFAULT_K = 7;
  static const int DEFAULT_M = 3;
  static const int g_decode_tbls_lru_length=2516; // caches up to 12+4 completely

  unsigned char* a; // encoding coefficient
  unsigned char* g_encode_tbls; // encoding table


  // we create a cache for decoding tables
  Mutex g_decode_tbls_guard;

  int get_tbls_lru_size()
  {
    Mutex::Locker lock(g_decode_tbls_guard);
    return g_decode_tbls_lru.size();
  }

  // we implement an LRU cache for coding matrix - the cache size is sufficient up to (10,4) decodings
  typedef std::pair<std::list<std::string>::iterator, bufferptr> lru_entry_t;

  std::map<std::string, lru_entry_t> g_decode_tbls_map;
  std::list<std::string> g_decode_tbls_lru;

  ErasureCodeIsaDefault(int matrix = kVandermonde) : ErasureCodeIsa("default"),
    a(0), g_encode_tbls(0), g_decode_tbls_guard("isa-lru-cache")
  {
    matrixtype = matrix;
  }

  virtual
  ~ErasureCodeIsaDefault()
  {
    if (a) {
      free(a);
    }
    if (g_encode_tbls) {
      free(g_encode_tbls);
    }
  }

  virtual void isa_encode(char **data,
                          char **coding,
                          int blocksize);

  virtual bool erasure_contains(int *erasures, int i);

  virtual int isa_decode(int *erasures,
                         char **data,
                         char **coding,
                         int blocksize);

  virtual unsigned get_alignment() const;

  virtual void parse(const map<std::string, std::string> &parameters);

  virtual void prepare();

  bool get_decoding_table_from_cache(std::string &signature, unsigned char* &table);
  void put_decoding_table_to_cache(std::string&, unsigned char*&);
};

#endif
