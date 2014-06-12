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
#include <vector>
#include <algorithm>

#include "ErasureCode.h"

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

int ErasureCode::encode_prepare(const bufferlist &raw,
                                bufferlist *prepared) const
{
  unsigned int k = get_data_chunk_count();
  unsigned int m = get_chunk_count() - k;
  unsigned blocksize = get_chunk_size(raw.length());
  unsigned padded_length = blocksize * k;
  *prepared = raw;
  if (padded_length - raw.length() > 0) {
    bufferptr pad(padded_length - raw.length());
    pad.zero();
    prepared->push_back(pad);
  }
  unsigned coding_length = blocksize * m;
  bufferptr coding(buffer::create_page_aligned(coding_length));
  prepared->push_back(coding);
  prepared->rebuild_page_aligned();
  return 0;
}

int ErasureCode::encode(const set<int> &want_to_encode,
                        const bufferlist &in,
                        map<int, bufferlist> *encoded)
{
  unsigned int k = get_data_chunk_count();
  unsigned int m = get_chunk_count() - k;
  bufferlist out;
  int err = encode_prepare(in, &out);
  if (err)
    return err;
  unsigned blocksize = get_chunk_size(in.length());
  for (unsigned int i = 0; i < k + m; i++) {
    bufferlist &chunk = (*encoded)[i];
    chunk.substr_of(out, i * blocksize, blocksize);
  }
  encode_chunks(want_to_encode, encoded);
  for (unsigned int i = 0; i < k + m; i++) {
    if (want_to_encode.count(i) == 0)
      encoded->erase(i);
  }
  return 0;
}

int ErasureCode::encode_chunks(const set<int> &want_to_encode,
                               map<int, bufferlist> *encoded)
{
  assert("ErasureCode::encode_chunks not implemented" == 0);
}
 
int ErasureCode::decode(const set<int> &want_to_read,
                        const map<int, bufferlist> &chunks,
                        map<int, bufferlist> *decoded)
{
  vector<int> have;
  have.reserve(chunks.size());
  for (map<int, bufferlist>::const_iterator i = chunks.begin();
       i != chunks.end();
       ++i) {
    have.push_back(i->first);
  }
  if (includes(
	have.begin(), have.end(), want_to_read.begin(), want_to_read.end())) {
    for (set<int>::iterator i = want_to_read.begin();
	 i != want_to_read.end();
	 ++i) {
      (*decoded)[*i] = chunks.find(*i)->second;
    }
    return 0;
  }
  unsigned int k = get_data_chunk_count();
  unsigned int m = get_chunk_count() - k;
  unsigned blocksize = (*chunks.begin()).second.length();
  for (unsigned int i =  0; i < k + m; i++) {
    if (chunks.find(i) == chunks.end()) {
      bufferptr ptr(buffer::create_page_aligned(blocksize));
      (*decoded)[i].push_front(ptr);
    } else {
      (*decoded)[i] = chunks.find(i)->second;
      (*decoded)[i].rebuild_page_aligned();
    }
  }
  return decode_chunks(want_to_read, chunks, decoded);
}

int ErasureCode::decode_chunks(const set<int> &want_to_read,
                               const map<int, bufferlist> &chunks,
                               map<int, bufferlist> *decoded)
{
  assert("ErasureCode::decode_chunks not implemented" == 0);
}


