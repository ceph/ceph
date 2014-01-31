// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 CERN/Switzerland
 *
 * Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

// -----------------------------------------------------------------------------

#include "arch/intel.h"
#include "common/debug.h"
#include "ErasureCodeXor.h"
#include "xor.h"

// -----------------------------------------------------------------------------

#include <errno.h>

// -----------------------------------------------------------------------------

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

// -----------------------------------------------------------------------------

static ostream&
_prefix (std::ostream* _dout)
{
  return *_dout << "ErasureCodeXor: ";
}


// -----------------------------------------------------------------------------

void
ErasureCodeXor::init (const std::map<std::string, std::string> &parameters)
{
  dout(10) << "technique=xor(raid4)" << dendl;
  parse(parameters);
}

// -----------------------------------------------------------------------------

unsigned int
ErasureCodeXor::get_chunk_size (unsigned int object_size) const
{
  unsigned int alignment = get_alignment();
  unsigned int tail = object_size % alignment;
  unsigned int padded_length = object_size + (tail ? (alignment - tail) : 0);
  assert(padded_length % k == 0);
  return padded_length / k;
}

// -----------------------------------------------------------------------------

int
ErasureCodeXor::minimum_to_decode (const std::set<int> &want_to_read,
                                   const std::set<int> &available_chunks,
                                   std::set<int> *minimum)
{
  if (includes(available_chunks.begin(), available_chunks.end(),
               want_to_read.begin(), want_to_read.end())) {
    *minimum = want_to_read;
  }
  else {
    if (available_chunks.size() < (unsigned) k)
      return -EIO;
    std::set<int>::iterator i;
    unsigned int j;
    for (i = available_chunks.begin(), j = 0; j < (unsigned int) k; ++i, j++)
      minimum->insert(*i);
  }
  return 0;
}

// -----------------------------------------------------------------------------

int
ErasureCodeXor::minimum_to_decode_with_cost (const std::set<int> &want_to_read,
                                             const std::map<int, int> &available,
                                             std::set<int> *minimum)
{
  std::set <int> available_chunks;
  for (std::map<int, int>::const_iterator i = available.begin();
    i != available.end();
    ++i)
    available_chunks.insert(i->first);
  return minimum_to_decode(want_to_read, available_chunks, minimum);
}

// -----------------------------------------------------------------------------

int
ErasureCodeXor::encode_chunks (vector<bufferlist> &chunks)
{
  assert(chunks.size() == (unsigned int) (k + 1));
  int i = 0;

  std::set<vector_op_t*> data;
  vector_op_t* coding = 0;

  for (vector<bufferlist>::iterator chunk = chunks.begin();
    chunk != chunks.end();
    chunk++, i++) {
    if (i < k)
      data.insert((vector_op_t*) ((uintptr_t) (chunk->c_str())));
    else
      coding = (vector_op_t*) ((uintptr_t) (chunk->c_str()));
  }
  compute(data, coding, chunks.front().length());
  return 0;
}

// -----------------------------------------------------------------------------

int
ErasureCodeXor::decode_chunks (vector<bool> erasures,
                               vector<bufferlist> &chunks)
{
  assert(chunks.size() == (unsigned int) (k + 1));
  unsigned int missing = 0;
  for (vector<bool>::const_iterator it = erasures.begin();
    it != erasures.end(); ++it) {
    if (*it)
      missing++;
  }

  // ---------------------------------------------------------------------------
  // if more than 1 chunk is missing we can not decode
  // ---------------------------------------------------------------------------
  if (missing > 1)
    return -EIO;

  // ---------------------------------------------------------------------------
  // if nothing is missing we don't need to do anything
  // ---------------------------------------------------------------------------
  if (missing == 0)
    return 0;

  std::set<vector_op_t*> data;
  vector_op_t* coding = 0;

  for (unsigned int i = 0; i < chunks.size(); i++) {
    if (erasures[i])
      coding = (vector_op_t*) ((uintptr_t) chunks[i].c_str());
    else
      data.insert((vector_op_t*) ((uintptr_t) chunks[i].c_str()));
  }

  compute(data, coding, chunks.front().length());
  return 0;
}

// -----------------------------------------------------------------------------

unsigned int
ErasureCodeXor::get_alignment () const
{
  // ---------------------------------------------------------------------------
  // if we have sse2 extensions we require 64-byte alignment to use region XOR
  // otherwise we require alignment to the largest possible vector operations
  // ---------------------------------------------------------------------------
  unsigned int alignment = (ceph_arch_intel_sse2) ?
    (k * 64) : (k * LARGEST_VECTOR_WORDSIZE);

  return alignment;
}

// -----------------------------------------------------------------------------

void
ErasureCodeXor::parse (const map<std::string, std::string> &parameters)
{
  k = to_int("erasure-code-k", parameters, DEFAULT_K);
}

int
ErasureCodeXor::to_int (const std::string &name,
                        const map<std::string, std::string> &parameters,
                        int default_value)
{
  if (parameters.find(name) == parameters.end() ||
      parameters.find(name)->second.size() == 0) {
    dout(10) << name << " defaults to " << default_value << dendl;
    return default_value;
  }
  const std::string value = parameters.find(name)->second;
  std::string p = value;
  std::string err;
  int r = strict_strtol(p.c_str(), 10, &err);
  if (!err.empty()) {
    derr << "could not convert " << name << "=" << value
      << " to int because " << err
      << ", set to default " << default_value << dendl;
    return default_value;
  }
  dout(10) << name << " set to " << r << dendl;
  return r;
}

// -----------------------------------------------------------------------------

void
ErasureCodeXor::compute (const std::set<vector_op_t*> data,
                         vector_op_t* parity,
                         int blocksize)
{
  if (ceph_arch_intel_sse2) {
    // -------------------------------------------------------------------------
    // if available call the SSE2 region XOR implementation
    // -------------------------------------------------------------------------
    vector_sse_xor(data, parity, blocksize);
  }
  else {
    // -------------------------------------------------------------------------
    // if no SSE2 is available do chunk by chunk xor'ing with vector operations
    // -------------------------------------------------------------------------
    bool append = false;
    for (std::set<vector_op_t*>::const_iterator it = data.begin();
      it != data.end();
      ++it, append = true) {

      if (append)
        vector_xor(parity, *it, blocksize / VECTOR_WORDSIZE);
      else
        vector_assign(parity, *it, blocksize / VECTOR_WORDSIZE);
    }
  }
}
