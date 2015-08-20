// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef CEPH_ERASURE_CODE_EXAMPLE_H
#define CEPH_ERASURE_CODE_EXAMPLE_H

#include <unistd.h>
#include <errno.h>
#include <algorithm>
#include <sstream>

#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"
#include "erasure-code/ErasureCode.h"

#define FIRST_DATA_CHUNK 0
#define SECOND_DATA_CHUNK 1
#define DATA_CHUNKS 2u

#define CODING_CHUNK 2
#define CODING_CHUNKS 1u

#define MINIMUM_TO_RECOVER 2u

class ErasureCodeExample : public ErasureCode {
public:
  virtual ~ErasureCodeExample() {}

  virtual int create_ruleset(const string &name,
			     CrushWrapper &crush,
			     ostream *ss) const {
    return crush.add_simple_ruleset(name, "default", "host",
				    "indep", pg_pool_t::TYPE_ERASURE, ss);
  }
  
  virtual int minimum_to_decode(const set<int> &want_to_read,
                                const set<int> &available_chunks,
                                set<int> *minimum) {
    if (includes(available_chunks.begin(), available_chunks.end(),
		 want_to_read.begin(), want_to_read.end())) {
      *minimum = want_to_read;
      return 0;
    } else if (available_chunks.size() >= MINIMUM_TO_RECOVER) {
      *minimum = available_chunks;
      return 0;
    } else {
      return -EIO;
    }
  }

  virtual int minimum_to_decode_with_cost(const set<int> &want_to_read,
                                          const map<int, int> &available,
                                          set<int> *minimum) {
    //
    // If one chunk is more expensive to fetch than the others,
    // recover it instead. For instance, if the cost reflects the
    // time it takes for a chunk to be retrieved from a remote
    // OSD and if CPU is cheap, it could make sense to recover
    // instead of fetching the chunk.
    //
    map<int, int> c2c(available);
    if (c2c.size() > DATA_CHUNKS) {
      if (c2c[FIRST_DATA_CHUNK] > c2c[SECOND_DATA_CHUNK] &&
	  c2c[FIRST_DATA_CHUNK] > c2c[CODING_CHUNK])
	c2c.erase(FIRST_DATA_CHUNK);
      else if(c2c[SECOND_DATA_CHUNK] > c2c[FIRST_DATA_CHUNK] &&
	      c2c[SECOND_DATA_CHUNK] > c2c[CODING_CHUNK])
	c2c.erase(SECOND_DATA_CHUNK);
      else if(c2c[CODING_CHUNK] > c2c[FIRST_DATA_CHUNK] &&
	      c2c[CODING_CHUNK] > c2c[SECOND_DATA_CHUNK])
	c2c.erase(CODING_CHUNK);
    }
    set <int> available_chunks;
    for (map<int, int>::const_iterator i = c2c.begin();
	 i != c2c.end();
	 ++i)
      available_chunks.insert(i->first);
    return minimum_to_decode(want_to_read, available_chunks, minimum);
  }

  virtual unsigned int get_chunk_count() const {
    return DATA_CHUNKS + CODING_CHUNKS;
  }

  virtual unsigned int get_data_chunk_count() const {
    return DATA_CHUNKS;
  }

  virtual unsigned int get_chunk_size(unsigned int object_size) const {
    return ( object_size / DATA_CHUNKS ) + 1;
  }

  virtual int encode(const set<int> &want_to_encode,
                     const bufferlist &in,
                     map<int, bufferlist> *encoded) {
    //
    // make sure all data chunks have the same length, allocating
    // padding if necessary.
    //
    unsigned int chunk_length = get_chunk_size(in.length());
    bufferlist out(in);
    unsigned int width = get_chunk_count() * get_chunk_size(in.length());
    bufferptr pad(width - in.length());
    pad.zero(0, get_data_chunk_count());
    out.push_back(pad);
    //
    // compute the coding chunk with first chunk ^ second chunk
    //
    char *p = out.c_str();
    for (unsigned i = 0; i < chunk_length; i++)
      p[i + CODING_CHUNK * chunk_length] =
        p[i + FIRST_DATA_CHUNK * chunk_length] ^
	p[i + SECOND_DATA_CHUNK * chunk_length];
    //
    // populate the bufferlist with bufferptr pointing
    // to chunk boundaries
    //
    const bufferptr &ptr = out.front();
    for (set<int>::iterator j = want_to_encode.begin();
         j != want_to_encode.end();
         ++j) {
      bufferptr chunk(ptr, (*j) * chunk_length, chunk_length);
      (*encoded)[*j].push_front(chunk);
    }
    return 0;
  }

  virtual int encode_chunks(const set<int> &want_to_encode,
			    map<int, bufferlist> *encoded) {
    assert(0);
    return 0;
  }

  virtual int decode(const set<int> &want_to_read,
                     const map<int, bufferlist> &chunks,
                     map<int, bufferlist> *decoded) {
    //
    // All chunks have the same size
    //
    unsigned chunk_length = (*chunks.begin()).second.length();
    for (set<int>::iterator i = want_to_read.begin();
         i != want_to_read.end();
         ++i) {
      if (chunks.find(*i) != chunks.end()) {
	//
	// If the chunk is available, just copy the bufferptr pointer
	// to the decoded argument.
	//
        (*decoded)[*i] = chunks.find(*i)->second;
      } else if(chunks.size() != 2) {
	//
	// If a chunk is missing and there are not enough chunks
	// to recover, abort.
	//
	return -ERANGE;
      } else {
	//
	// No matter what the missing chunk is, XOR of the other
	// two recovers it.
	//
        map<int, bufferlist>::const_iterator k = chunks.begin();
        const char *a = k->second.front().c_str();
        ++k;
        const char *b = k->second.front().c_str();
        bufferptr chunk(chunk_length);
	char *c = chunk.c_str();
        for (unsigned j = 0; j < chunk_length; j++) {
          c[j] = a[j] ^ b[j];
        }
        (*decoded)[*i].push_front(chunk);
      }
    }
    return 0;
  }

  virtual int decode_chunks(const set<int> &want_to_read,
			    const map<int, bufferlist> &chunks,
			    map<int, bufferlist> *decoded) {
    assert(0);
    return 0;
  }

  virtual const vector<int> &get_chunk_mapping() const {
    static vector<int> mapping;
    return mapping;
  }

};

#endif
