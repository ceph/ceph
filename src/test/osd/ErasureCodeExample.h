// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
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
#include <sstream>
#include "osd/ErasureCodeInterface.h"

#define DATA_CHUNKS 2u
#define CODING_CHUNKS 1u

class ErasureCodeExample : public ErasureCodeInterface {
public:
  useconds_t delay;
  ErasureCodeExample(const map<std::string,std::string> &parameters) :
    delay(0)
  {
    if (parameters.find("usleep") != parameters.end()) {
      std::istringstream ss(parameters.find("usleep")->second);
      ss >> delay;
      usleep(delay);
    }
  }

  virtual ~ErasureCodeExample() {}
  
  virtual int minimum_to_decode(const set<int> &want_to_read,
                                const set<int> &available_chunks,
                                set<int> *minimum) {
    if (available_chunks.size() < DATA_CHUNKS)
      return -EIO;
    set<int>::iterator i;
    unsigned j;
    for (i = available_chunks.begin(), j = 0; j < DATA_CHUNKS; i++, j++)
      minimum->insert(*i);
    return 0;
  }

  virtual int minimum_to_decode_with_cost(const set<int> &want_to_read,
                                          const map<int, int> &available,
                                          set<int> *minimum) {
    set <int> available_chunks;
    for (map<int, int>::const_iterator i = available.begin();
	 i != available.end();
	 i++)
      available_chunks.insert(i->first);
    return minimum_to_decode(want_to_read, available_chunks, minimum);
  }

  virtual int encode(const set<int> &want_to_encode,
                     const bufferlist &in,
                     map<int, bufferlist> *encoded) {
    unsigned chunk_length = ( in.length() / DATA_CHUNKS ) + 1;
    unsigned length = chunk_length * ( DATA_CHUNKS + CODING_CHUNKS );
    bufferlist out(in);
    bufferptr pad(length - in.length());
    pad.zero(0, DATA_CHUNKS);
    out.push_back(pad);
    char *p = out.c_str();
    for (unsigned i = 0; i < chunk_length * DATA_CHUNKS; i++)
      p[i + 2 * chunk_length] =
        p[i + 0 * chunk_length] ^ p[i + 1 * chunk_length];
    const bufferptr ptr = out.buffers().front();
    for (set<int>::iterator j = want_to_encode.begin();
         j != want_to_encode.end();
         j++) {
      bufferptr chunk(ptr, (*j) * chunk_length, chunk_length);
      (*encoded)[*j].push_front(chunk);
    }
    return 0;
  }

  virtual int decode(const set<int> &want_to_read,
                     const map<int, bufferlist> &chunks,
                     map<int, bufferlist> *decoded) {
    
    unsigned chunk_length = (*chunks.begin()).second.length();
    for (set<int>::iterator i = want_to_read.begin();
         i != want_to_read.end();
         i++) {
      if (chunks.find(*i) != chunks.end())
        (*decoded)[*i] = chunks.find(*i)->second;
      else {
        bufferptr chunk(chunk_length);
        map<int, bufferlist>::const_iterator k = chunks.begin();
        const char *a = k->second.buffers().front().c_str();
        k++;
        const char *b = k->second.buffers().front().c_str();
        for (unsigned j = 0; j < chunk_length; j++) {
          chunk[j] = a[j] ^ b[j];
        }
        (*decoded)[*i].push_front(chunk);
      }
    }
    return 0;
  }
};

#endif
