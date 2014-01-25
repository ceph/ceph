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

#include <errno.h>
#include <algorithm>

#include "common/debug.h"
#include "osd/ErasureCode.h"

// re-include our assert
#include "include/assert.h" 

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCode: ";
}

int ErasureCode::minimum_to_decode(const set<int> &want_to_read,
                                   const set<int> &available_chunks,
                                   set<int> *minimum) 
{
  if (includes(available_chunks.begin(), available_chunks.end(),
	       want_to_read.begin(), want_to_read.end())) {
    *minimum = want_to_read;
  } else {
    unsigned int k = get_data_chunk_count();
    if (available_chunks.size() < (unsigned)k)
      return -EIO;
    set<int>::iterator i;
    unsigned j;
    for (i = available_chunks.begin(), j = 0; j < (unsigned)k; ++i, j++)
      minimum->insert(*i);
  }
  return 0;
}

int ErasureCode::minimum_to_decode_with_cost(const set<int> &want_to_read,
                                             const map<int, int> &available,
                                             set<int> *minimum)
{
  set <int> available_chunks;
  for (map<int, int>::const_iterator i = available.begin();
       i != available.end();
       ++i)
    available_chunks.insert(i->first);
  return minimum_to_decode(want_to_read, available_chunks, minimum);
}

int ErasureCode::encode(const set<int> &want_to_encode,
                        const bufferlist &in,
                        map<int, bufferlist> *encoded)
{
  unsigned blocksize = get_chunk_size(in.length());
  unsigned int k = get_data_chunk_count();
  unsigned int m = get_chunk_count() - k;
  unsigned padded_length = blocksize * k;
  bufferlist out(in);
  if (padded_length - in.length() > 0) {
    dout(10) << "encode adjusted buffer length from " << in.length()
	     << " to " << padded_length << dendl;
    bufferptr pad(padded_length - in.length());
    pad.zero();
    out.push_back(pad);
    out.rebuild_page_aligned();
  }
  unsigned coding_length = blocksize * m;
  bufferptr coding(buffer::create_page_aligned(coding_length));
  out.push_back(coding);
  vector<bufferlist> chunks;
  for (unsigned int i = 0; i < k + m; i++) {
    bufferlist &chunk = (*encoded)[i];
    chunk.substr_of(out, i * blocksize, blocksize);
    chunks.push_back(chunk);
  }
  encode_chunks(chunks);
  for (unsigned int i = 0; i < k + m; i++)
    if (want_to_encode.count(i) == 0)
      encoded->erase(i);
  return 0;
}

int ErasureCode::decode(const set<int> &want_to_read,
                        const map<int, bufferlist> &chunks,
                        map<int, bufferlist> *decoded)
{
  unsigned int k = get_data_chunk_count();
  unsigned int m = get_chunk_count() - k;
  unsigned blocksize = chunks.begin()->second.length();
  vector<bufferlist> buffers;
  vector<bool> erasures;
  for (unsigned int i =  0; i < k + m; i++) {
    if (chunks.find(i) == chunks.end()) {
      erasures.push_back(true);
      if (decoded->find(i) == decoded->end() ||
	  decoded->find(i)->second.length() != blocksize) {
	bufferptr ptr(buffer::create_page_aligned(blocksize));
	(*decoded)[i].push_front(ptr);
      }
    } else {
      erasures.push_back(false);
      (*decoded)[i] = chunks.find(i)->second;
    }
    buffers.push_back((*decoded)[i]);
  }

  int r = decode_chunks(erasures, buffers);
  if (r)
    return r;

  for (unsigned int i = 0; i < k + m; i++)
    if (want_to_read.count(i) == 0)
      decoded->erase(i);
  return 0;
}


int ErasureCode::decode_concat(const map<int, bufferlist> &chunks,
                               bufferlist *decoded)
{
  set<int> want_to_read;
  for (unsigned int i = 0; i < get_data_chunk_count(); i++)
    want_to_read.insert(i);
  set<int> available;
  for (map<int,bufferlist>::const_iterator i = chunks.begin();
       i != chunks.end();
       i++)
    available.insert(i->first);
  set<int> minimum;
  int r = minimum_to_decode(want_to_read, available, &minimum);
  if (r)
    return r;
  map<int, bufferlist> decoded_map;
  r = decode(want_to_read, chunks, &decoded_map);
  if (r == 0)
    for (unsigned int i = 0; i < get_data_chunk_count(); i++)
      decoded->claim_append(decoded_map[i]);
  return r;
}
