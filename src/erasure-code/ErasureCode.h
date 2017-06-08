// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef CEPH_ERASURE_CODE_H
#define CEPH_ERASURE_CODE_H

/*! @file ErasureCode.h
    @brief Base class for erasure code plugins implementors

 */ 

#include <vector>

#include "ErasureCodeInterface.h"

namespace ceph {

  class ErasureCode : public ErasureCodeInterface {
  public:
    static const unsigned SIMD_ALIGN;

    vector<int> chunk_mapping;
    ErasureCodeProfile _profile;

    virtual ~ErasureCode() {}

    virtual int init(ErasureCodeProfile &profile, ostream *ss) {
      _profile = profile;
      return 0;
    }

    virtual const ErasureCodeProfile &get_profile() const {
      return _profile;
    }

    int sanity_check_k(int k, ostream *ss);

    virtual unsigned int get_coding_chunk_count() const {
      return get_chunk_count() - get_data_chunk_count();
    }

    virtual int minimum_to_decode(const set<int> &want_to_read,
                                  const set<int> &available_chunks,
                                  set<int> *minimum);

    virtual int minimum_to_decode_with_cost(const set<int> &want_to_read,
                                            const map<int, int> &available,
                                            set<int> *minimum);

    int encode_prepare(const bufferlist &raw,
                       map<int, bufferlist> &encoded) const;

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

    virtual const vector<int> &get_chunk_mapping() const;

    int to_mapping(const ErasureCodeProfile &profile,
		   ostream *ss);

    static int to_int(const std::string &name,
		      ErasureCodeProfile &profile,
		      int *value,
		      const std::string &default_value,
		      ostream *ss);

    static int to_bool(const std::string &name,
		       ErasureCodeProfile &profile,
		       bool *value,
		       const std::string &default_value,
		       ostream *ss);

    static int to_string(const std::string &name,
			 ErasureCodeProfile &profile,
			 std::string *value,
			 const std::string &default_value,
			 ostream *ss);

    virtual int decode_concat(const map<int, bufferlist> &chunks,
			      bufferlist *decoded);

  protected:
    int parse(const ErasureCodeProfile &profile,
	      ostream *ss);

  private:
    int chunk_index(unsigned int i) const;
  };
}

#endif
