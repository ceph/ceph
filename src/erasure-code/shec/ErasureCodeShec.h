// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 FUJITSU LIMITED
 * Copyright (C) 2013, 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Takanori Nakao <nakao.takanori@jp.fujitsu.com>
 * Author: Takeshi Miyamae <miyamae.takeshi@jp.fujitsu.com>
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_ERASURE_CODE_SHEC_H
#define CEPH_ERASURE_CODE_SHEC_H

#include "common/Mutex.h"
#include "erasure-code/ErasureCode.h"
#include "ErasureCodeShecTableCache.h"
#include <list>

class ErasureCodeShec : public ErasureCode {

public:
  enum {
    MULTIPLE = 0,
    SINGLE = 1
  };

  ErasureCodeShecTableCache &tcache;
  int k;
  int DEFAULT_K;
  int m;
  int DEFAULT_M;
  int c;
  int DEFAULT_C;
  int w;
  int DEFAULT_W;
  int technique;
  string ruleset_root;
  string ruleset_failure_domain;
  int *matrix;

  ErasureCodeShec(const int _technique,
		  ErasureCodeShecTableCache &_tcache) :
    tcache(_tcache),
    k(0),
    DEFAULT_K(2),
    m(0),
    DEFAULT_M(1),
    c(0),
    DEFAULT_C(1),
    w(0),
    DEFAULT_W(8),
    technique(_technique),
    ruleset_root("default"),
    ruleset_failure_domain("host"),
    matrix(0)
  {}

  virtual ~ErasureCodeShec() {}

  virtual int create_ruleset(const string &name,
			     CrushWrapper &crush,
			     ostream *ss) const;

  virtual unsigned int get_chunk_count() const {
    return k + m;
  }

  virtual unsigned int get_data_chunk_count() const {
    return k;
  }

  virtual unsigned int get_chunk_size(unsigned int object_size) const;

  virtual int minimum_to_decode(const set<int> &want_to_decode,
				const set<int> &available_chunks,
				set<int> *minimum);

  virtual int minimum_to_decode_with_cost(const set<int> &want_to_decode,
					  const map<int, int> &available,
					  set<int> *minimum);

  virtual int encode(const set<int> &want_to_encode,
		     const bufferlist &in,
		     map<int, bufferlist> *encoded);
  virtual int encode_chunks(const set<int> &want_to_encode,
			    map<int, bufferlist> *encoded);

  virtual int decode(const set<int> &want_to_read,
		     const map<int, bufferlist> &chunks,
		     map<int, bufferlist> *decoded);
  virtual int decode_chunks(const set<int> &want_to_read,
			    const map<int, bufferlist> &chunks,
			    map<int, bufferlist> *decoded);

  int init(const map<std::string,std::string> &parameters);
  virtual void shec_encode(char **data,
			   char **coding,
			   int blocksize) = 0;
  virtual int shec_decode(int *erasures,
			  int *avails,
			  char **data,
			  char **coding,
			  int blocksize) = 0;
  virtual unsigned get_alignment() const = 0;
  virtual int parse(const map<std::string,std::string> &parameters) = 0;
  virtual void prepare() = 0;
};

class ErasureCodeShecReedSolomonVandermonde : public ErasureCodeShec {
public:

  ErasureCodeShecReedSolomonVandermonde(ErasureCodeShecTableCache &_tcache,
					int technique = MULTIPLE) :
    ErasureCodeShec(technique, _tcache)
  {}

  virtual ~ErasureCodeShecReedSolomonVandermonde() {
  }

  virtual void shec_encode(char **data,
			   char **coding,
			   int blocksize);
  virtual int shec_decode(int *erasures,
			  int *avails,
			  char **data,
			  char **coding,
			  int blocksize);
  virtual unsigned get_alignment() const;
  virtual int parse(const map<std::string,std::string> &parameters);
  virtual void prepare();
};

#endif
