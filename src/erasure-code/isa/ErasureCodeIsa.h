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
#include "erasure-code/ErasureCode.h"
#include "ErasureCodeIsaTableCache.h"
// -----------------------------------------------------------------------------
#include <list>
// -----------------------------------------------------------------------------

#define DEFAULT_RULESET_ROOT "default"
#define DEFAULT_RULESET_FAILURE_DOMAIN "host"

class ErasureCodeIsa : public ErasureCode {
public:

  enum eMatrix {
    kVandermonde = 0, kCauchy = 1
  };

  int k;
  int m;
  int w;

  ErasureCodeIsaTableCache &tcache;
  const char *technique;
  string ruleset_root;
  string ruleset_failure_domain;

  ErasureCodeIsa(const char *_technique,
                 ErasureCodeIsaTableCache &_tcache) :
  k(0),
  m(0),
  w(0),
  tcache(_tcache),
  technique(_technique),
  ruleset_root(DEFAULT_RULESET_ROOT),
  ruleset_failure_domain(DEFAULT_RULESET_FAILURE_DOMAIN)
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

  virtual int encode_chunks(const set<int> &want_to_encode,
                            map<int, bufferlist> *encoded);

  virtual int decode_chunks(const set<int> &want_to_read,
                            const map<int, bufferlist> &chunks,
                            map<int, bufferlist> *decoded);

  virtual int init(ErasureCodeProfile &profile, ostream *ss);

  virtual void isa_encode(char **data,
                          char **coding,
                          int blocksize) = 0;


  virtual int isa_decode(int *erasures,
                         char **data,
                         char **coding,
                         int blocksize) = 0;

  virtual unsigned get_alignment() const = 0;

  virtual void prepare() = 0;

 private:
  virtual int parse(ErasureCodeProfile &profile,
                    ostream *ss) = 0;
};

// -----------------------------------------------------------------------------

class ErasureCodeIsaDefault : public ErasureCodeIsa {
private:
  int matrixtype;

public:

  static const std::string DEFAULT_K;
  static const std::string DEFAULT_M;

  unsigned char* encode_coeff; // encoding coefficient
  unsigned char* encode_tbls; // encoding table

  ErasureCodeIsaDefault(ErasureCodeIsaTableCache &_tcache,
                        int matrix = kVandermonde) :

  ErasureCodeIsa("default", _tcache),
  encode_coeff(0), encode_tbls(0)
  {
    matrixtype = matrix;
  }

  virtual
  ~ErasureCodeIsaDefault()
  {

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

  virtual void prepare();

 private:
  virtual int parse(ErasureCodeProfile &profile,
                    ostream *ss);
};

#endif
