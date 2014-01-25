// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
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
#include "osd/ErasureCode.h"

#define FIRST_DATA_CHUNK 0
#define SECOND_DATA_CHUNK 1
#define DATA_CHUNKS 2u

#define CODING_CHUNK 2
#define CODING_CHUNKS 1u

#define MINIMUM_TO_RECOVER 2u

class ErasureCodeExample : public ErasureCode {
public:
  virtual ~ErasureCodeExample() {}
  
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

  virtual int encode_chunks(vector<bufferlist> &chunks) {
    //
    // compute the coding chunk with first chunk ^ second chunk
    //
    unsigned int chunk_length = chunks[FIRST_DATA_CHUNK].length();
    char *first = chunks[FIRST_DATA_CHUNK].c_str();
    char *second = chunks[SECOND_DATA_CHUNK].c_str();
    char *coding = chunks[CODING_CHUNK].c_str();
    for (unsigned int i = 0; i < chunk_length; i++)
      coding[i] = first[i] ^ second[i];
    return 0;
  }

  virtual int decode_chunks(vector<bool> erasures,
			    vector<bufferlist> &chunks) {
    int erasure = -1;
    for (unsigned int i = 0; i < erasures.size(); i++)
      if (erasures[i])
	erasure = i;
    if (erasure < 0)
      return 0;
    char *destination = chunks[erasure].c_str();
    const char *one = chunks[(erasure + 1) % erasures.size()].c_str();
    const char *two = chunks[(erasure + 2) % erasures.size()].c_str();
    unsigned chunk_length = chunks[0].length();
    for (unsigned int i = 0; i < chunk_length; i++)
      destination[i] = one[i] ^ two[i];
    return 0;
  }
};

#endif
