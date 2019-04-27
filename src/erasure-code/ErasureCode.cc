// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include <algorithm>
#include <cerrno>

#include "ErasureCode.h"

#include "common/strtol.h"
#include "include/buffer.h"
#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"

#define DEFAULT_RULE_ROOT "default"
#define DEFAULT_RULE_FAILURE_DOMAIN "host"

using std::make_pair;
using std::map;
using std::ostream;
using std::pair;
using std::set;
using std::string;
using std::vector;

using ceph::bufferlist;

namespace ceph {
const unsigned ErasureCode::SIMD_ALIGN = 32;

int ErasureCode::init(
  ErasureCodeProfile &profile,
  std::ostream *ss)
{
  int err = 0;
  err |= to_string("crush-root", profile,
		   &rule_root,
		   DEFAULT_RULE_ROOT, ss);
  err |= to_string("crush-failure-domain", profile,
		   &rule_failure_domain,
		   DEFAULT_RULE_FAILURE_DOMAIN, ss);
  err |= to_string("crush-device-class", profile,
		   &rule_device_class,
		   "", ss);
  if (err)
    return err;
  _profile = profile;
  return 0;
}

int ErasureCode::create_rule(
  const std::string &name,
  CrushWrapper &crush,
  std::ostream *ss) const
{
  int ruleid = crush.add_simple_rule(
    name,
    rule_root,
    rule_failure_domain,
    rule_device_class,
    "indep",
    pg_pool_t::TYPE_ERASURE,
    ss);

  if (ruleid < 0)
    return ruleid;

  crush.set_rule_mask_max_size(ruleid, get_chunk_count());
  return ruleid;
}

int ErasureCode::sanity_check_k_m(int k, int m, ostream *ss)
{
  if (k < 2) {
    *ss << "k=" << k << " must be >= 2" << std::endl;
    return -EINVAL;
  }
  if (m < 1) {
    *ss << "m=" << m << " must be >= 1" << std::endl;
    return -EINVAL;
  }
  return 0;
}

int ErasureCode::chunk_index(unsigned int i) const
{
  return chunk_mapping.size() > i ? chunk_mapping[i] : i;
}

int ErasureCode::_minimum_to_decode(const set<int> &want_to_read,
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

int ErasureCode::minimum_to_decode(const set<int> &want_to_read,
                                   const set<int> &available_chunks,
                                   map<int, vector<pair<int, int>>> *minimum)
{
  set<int> minimum_shard_ids;
  int r = _minimum_to_decode(want_to_read, available_chunks, &minimum_shard_ids);
  if (r != 0) {
    return r;
  }
  vector<pair<int, int>> default_subchunks;
  default_subchunks.push_back(make_pair(0, get_sub_chunk_count()));
  for (auto &&id : minimum_shard_ids) {
    minimum->insert(make_pair(id, default_subchunks));
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
  return _minimum_to_decode(want_to_read, available_chunks, minimum);
}

int ErasureCode::encode_prepare(const bufferlist &raw,
                                map<int, bufferlist> &encoded) const
{
  unsigned int k = get_data_chunk_count();
  unsigned int m = get_chunk_count() - k;
  unsigned blocksize = get_chunk_size(raw.length());
  unsigned padded_chunks = k - raw.length() / blocksize;
  bufferlist prepared = raw;

  for (unsigned int i = 0; i < k - padded_chunks; i++) {
    bufferlist &chunk = encoded[chunk_index(i)];
    chunk.substr_of(prepared, i * blocksize, blocksize);
    chunk.rebuild_aligned_size_and_memory(blocksize, SIMD_ALIGN);
    ceph_assert(chunk.is_contiguous());
  }
  if (padded_chunks) {
    unsigned remainder = raw.length() - (k - padded_chunks) * blocksize;
    bufferptr buf(buffer::create_aligned(blocksize, SIMD_ALIGN));

    raw.copy((k - padded_chunks) * blocksize, remainder, buf.c_str());
    buf.zero(remainder, blocksize - remainder);
    encoded[chunk_index(k-padded_chunks)].push_back(std::move(buf));

    for (unsigned int i = k - padded_chunks + 1; i < k; i++) {
      bufferptr buf(buffer::create_aligned(blocksize, SIMD_ALIGN));
      buf.zero();
      encoded[chunk_index(i)].push_back(std::move(buf));
    }
  }
  for (unsigned int i = k; i < k + m; i++) {
    bufferlist &chunk = encoded[chunk_index(i)];
    chunk.push_back(buffer::create_aligned(blocksize, SIMD_ALIGN));
  }

  return 0;
}

int ErasureCode::encode(const set<int> &want_to_encode,
                        const bufferlist &in,
                        map<int, bufferlist> *encoded)
{
  unsigned int k = get_data_chunk_count();
  unsigned int m = get_chunk_count() - k;
  bufferlist out;
  int err = encode_prepare(in, *encoded);
  if (err)
    return err;
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
  ceph_abort_msg("ErasureCode::encode_chunks not implemented");
}
 
int ErasureCode::_decode(const set<int> &want_to_read,
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
      bufferlist tmp;
      bufferptr ptr(buffer::create_aligned(blocksize, SIMD_ALIGN));
      tmp.push_back(ptr);
      tmp.claim_append((*decoded)[i]);
      (*decoded)[i].swap(tmp);
    } else {
      (*decoded)[i] = chunks.find(i)->second;
      (*decoded)[i].rebuild_aligned(SIMD_ALIGN);
    }
  }
  return decode_chunks(want_to_read, chunks, decoded);
}

int ErasureCode::decode(const set<int> &want_to_read,
                        const map<int, bufferlist> &chunks,
                        map<int, bufferlist> *decoded, int chunk_size)
{
  return _decode(want_to_read, chunks, decoded);
}

int ErasureCode::decode_chunks(const set<int> &want_to_read,
                               const map<int, bufferlist> &chunks,
                               map<int, bufferlist> *decoded)
{
  ceph_abort_msg("ErasureCode::decode_chunks not implemented");
}

int ErasureCode::parse(const ErasureCodeProfile &profile,
		       ostream *ss)
{
  return to_mapping(profile, ss);
}

const vector<int> &ErasureCode::get_chunk_mapping() const {
  return chunk_mapping;
}

int ErasureCode::to_mapping(const ErasureCodeProfile &profile,
			    ostream *ss)
{
  if (profile.find("mapping") != profile.end()) {
    std::string mapping = profile.find("mapping")->second;
    int position = 0;
    vector<int> coding_chunk_mapping;
    for(std::string::iterator it = mapping.begin(); it != mapping.end(); ++it) {
      if (*it == 'D')
	chunk_mapping.push_back(position);
      else
	coding_chunk_mapping.push_back(position);
      position++;
    }
    chunk_mapping.insert(chunk_mapping.end(),
			 coding_chunk_mapping.begin(),
			 coding_chunk_mapping.end());
  }
  return 0;
}

int ErasureCode::to_int(const std::string &name,
			ErasureCodeProfile &profile,
			int *value,
			const std::string &default_value,
			ostream *ss)
{
  if (profile.find(name) == profile.end() ||
      profile.find(name)->second.size() == 0)
    profile[name] = default_value;
  std::string p = profile.find(name)->second;
  std::string err;
  int r = strict_strtol(p.c_str(), 10, &err);
  if (!err.empty()) {
    *ss << "could not convert " << name << "=" << p
	<< " to int because " << err
	<< ", set to default " << default_value << std::endl;
    *value = strict_strtol(default_value.c_str(), 10, &err);
    return -EINVAL;
  }
  *value = r;
  return 0;
}

int ErasureCode::to_bool(const std::string &name,
			 ErasureCodeProfile &profile,
			 bool *value,
			 const std::string &default_value,
			 ostream *ss)
{
  if (profile.find(name) == profile.end() ||
      profile.find(name)->second.size() == 0)
    profile[name] = default_value;
  const std::string p = profile.find(name)->second;
  *value = (p == "yes") || (p == "true");
  return 0;
}

int ErasureCode::to_string(const std::string &name,
			   ErasureCodeProfile &profile,
			   std::string *value,
			   const std::string &default_value,
			   ostream *ss)
{
  if (profile.find(name) == profile.end() ||
      profile.find(name)->second.size() == 0)
    profile[name] = default_value;
  *value = profile[name];
  return 0;
}

int ErasureCode::decode_concat(const map<int, bufferlist> &chunks,
			       bufferlist *decoded)
{
  set<int> want_to_read;

  for (unsigned int i = 0; i < get_data_chunk_count(); i++) {
    want_to_read.insert(chunk_index(i));
  }
  map<int, bufferlist> decoded_map;
  int r = _decode(want_to_read, chunks, &decoded_map);
  if (r == 0) {
    for (unsigned int i = 0; i < get_data_chunk_count(); i++) {
      decoded->claim_append(decoded_map[chunk_index(i)]);
    }
  }
  return r;
}
}
