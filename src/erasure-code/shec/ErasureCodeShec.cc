// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 FUJITSU LIMITED
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
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

#include <errno.h>
#include <algorithm>
#include "common/debug.h"
#include "ErasureCodeShec.h"
#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"
#include "shec.h"
extern "C" {
#include "jerasure/include/jerasure.h"
#include "jerasure/include/galois.h"
}

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodeShec: ";
}

int ErasureCodeShec::create_ruleset(const string &name,
				    CrushWrapper &crush,
				    ostream *ss) const
{
  int ruleid = crush.add_simple_ruleset(name, ruleset_root, ruleset_failure_domain,
					"indep", pg_pool_t::TYPE_ERASURE, ss);
  if (ruleid < 0) {
    return ruleid;
  } else {
    return crush.get_rule_mask_ruleset(ruleid);
  }
}

int ErasureCodeShec::init(const map<std::string,std::string> &parameters)
{
  dout(10) << "technique=" << technique << dendl;
  map<string,string>::const_iterator parameter;
  parameter = parameters.find("ruleset-root");
  if (parameter != parameters.end())
    ruleset_root = parameter->second;
  parameter = parameters.find("ruleset-failure-domain");
  if (parameter != parameters.end())
    ruleset_failure_domain = parameter->second;
  int err = parse(parameters);
  if (err) {
    return err;
  }
  prepare();
  return 0;
}

unsigned int ErasureCodeShec::get_chunk_size(unsigned int object_size) const
{
  unsigned alignment = get_alignment();
  unsigned tail = object_size % alignment;
  unsigned padded_length = object_size + ( tail ?  ( alignment - tail ) : 0 );

  assert(padded_length % k == 0);
  return padded_length / k;
}

int ErasureCodeShec::minimum_to_decode(const set<int> &want_to_decode,
				       const set<int> &available_chunks,
				       set<int> *minimum_chunks)
{
  if (!minimum_chunks) return -EINVAL;

  for (set<int>::iterator it = available_chunks.begin(); it != available_chunks.end(); ++it){
    if (*it < 0 || k+m <= *it) return -EINVAL;
  }

  if (includes(available_chunks.begin(), available_chunks.end(),
	       want_to_decode.begin(), want_to_decode.end())) {
    *minimum_chunks = want_to_decode;
  } else {
    int erased[k + m];
    int avails[k + m];
    int minimum[k + m];
    int dm_ids[k];

    for (int i = 0; i < k + m; i++) {
      erased[i] = 0;
      if (available_chunks.find(i) == available_chunks.end()) {
	if (want_to_decode.count(i) > 0) {
	  erased[i] = 1;
	}
	avails[i] = 0;
      } else {
	avails[i] = 1;
      }
    }

    if (shec_make_decoding_matrix(true, k, m, w, matrix, erased,
				  avails, 0, dm_ids, minimum) < 0) {
      return -EIO;
    }

    for (int i = 0; i < k + m; i++) {
      if (minimum[i] == 1) minimum_chunks->insert(i);
    }
  }

  return 0;
}

int ErasureCodeShec::minimum_to_decode_with_cost(const set<int> &want_to_decode,
						 const map<int, int> &available,
						 set<int> *minimum_chunks)
{
  set <int> available_chunks;

  for (map<int, int>::const_iterator i = available.begin();
       i != available.end();
       ++i)
    available_chunks.insert(i->first);

  return minimum_to_decode(want_to_decode, available_chunks, minimum_chunks);
}

int ErasureCodeShec::encode(const set<int> &want_to_encode,
			    const bufferlist &in,
			    map<int, bufferlist> *encoded)
{
  unsigned int k = get_data_chunk_count();
  unsigned int m = get_chunk_count() - k;
  bufferlist out;

  if (!encoded || !encoded->empty()){
    return -EINVAL;
  }

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

int ErasureCodeShec::encode_chunks(const set<int> &want_to_encode,
				   map<int, bufferlist> *encoded)
{
  char *chunks[k + m];
  for (int i = 0; i < k + m; i++){
    chunks[i] = (*encoded)[i].c_str();
  }
  shec_encode(&chunks[0], &chunks[k], (*encoded)[0].length());
  return 0;
}

int ErasureCodeShec::decode(const set<int> &want_to_read,
			    const map<int, bufferlist> &chunks,
			    map<int, bufferlist> *decoded)
{
  vector<int> have;

  if (!decoded || !decoded->empty()){
    return -EINVAL;
  }

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
      bufferptr ptr(buffer::create_aligned(blocksize, SIMD_ALIGN));
      (*decoded)[i].push_front(ptr);
    } else {
      (*decoded)[i] = chunks.find(i)->second;
      (*decoded)[i].rebuild_aligned(SIMD_ALIGN);
    }
  }
  return decode_chunks(want_to_read, chunks, decoded);
}

int ErasureCodeShec::decode_chunks(const set<int> &want_to_read,
				   const map<int, bufferlist> &chunks,
				   map<int, bufferlist> *decoded)
{
  unsigned blocksize = (*chunks.begin()).second.length();
  int erased[k + m];
  int erased_count = 0;
  int avails[k + m];
  char *data[k];
  char *coding[m];

  for (int i = 0; i < k + m; i++) {
    erased[i] = 0;
    if (chunks.find(i) == chunks.end()) {
      if (want_to_read.count(i) > 0) {
	erased[i] = 1;
	erased_count++;
      }
      avails[i] = 0;
    } else {
      (*decoded)[i] = chunks.find(i)->second;
      avails[i] = 1;
    }
    if (i < k)
      data[i] = (*decoded)[i].c_str();
    else
      coding[i - k] = (*decoded)[i].c_str();
  }

  if (erased_count > 0) {
    return shec_decode(erased, avails, data, coding, blocksize);
  } else {
    return 0;
  }
}

//
// ErasureCodeShecReedSolomonVandermonde
//

void ErasureCodeShecReedSolomonVandermonde::shec_encode(char **data,
					     char **coding,
					     int blocksize)
{
  jerasure_matrix_encode(k, m, w, matrix, data, coding, blocksize);
}

int ErasureCodeShecReedSolomonVandermonde::shec_decode(int *erased,
					    int *avails,
					    char **data,
					    char **coding,
					    int blocksize)
{
  return shec_matrix_decode(k, m, w, matrix,
			    erased, avails, data, coding, blocksize);
}

unsigned ErasureCodeShecReedSolomonVandermonde::get_alignment() const
{
  return k*w*sizeof(int);
}

int ErasureCodeShecReedSolomonVandermonde::parse(const map<std::string,std::string> &parameters)
{
  int err = 0;
  // k, m, c
  if (parameters.find("k") == parameters.end() &&
      parameters.find("m") == parameters.end() &&
      parameters.find("c") == parameters.end()){
    dout(10) << "(k, m, c) default to " << "(" << DEFAULT_K
	     << ", " << DEFAULT_M << ", " << DEFAULT_C << ")" << dendl;
    k = DEFAULT_K; m = DEFAULT_M; c = DEFAULT_C;
  } else if (parameters.find("k") == parameters.end() ||
	     parameters.find("m") == parameters.end() ||
	     parameters.find("c") == parameters.end()){
    dout(10) << "(k, m, c) must be choosed" << dendl;
    err = -EINVAL;
  } else {
    std::string err_k, err_m, err_c, value_k, value_m, value_c;
    value_k = parameters.find("k")->second;
    value_m = parameters.find("m")->second;
    value_c = parameters.find("c")->second;
    k = strict_strtol(value_k.c_str(), 10, &err_k);
    m = strict_strtol(value_m.c_str(), 10, &err_m);
    c = strict_strtol(value_c.c_str(), 10, &err_c);

    if (!err_k.empty() || !err_m.empty() || !err_c.empty()){
      if (!err_k.empty()){
	derr << "could not convert k=" << value_k << "to int" << dendl;
      } else if (!err_m.empty()){
	derr << "could not convert m=" << value_m << "to int" << dendl;
      } else if (!err_c.empty()){
	derr << "could not convert c=" << value_c << "to int" << dendl;
      }
      err = -EINVAL;
    } else if (k <= 0){
      derr << "k=" << k
	   << " must be a positive number" << dendl;
      err = -EINVAL;
    } else if (m <= 0){
      derr << "m=" << m
	   << " must be a positive number" << dendl;
      err = -EINVAL;
    } else if (c <= 0){
      derr << "c=" << c
	   << " must be a positive number" << dendl;
      err = -EINVAL;
    } else if (m < c){
      derr << "c=" << c
	   << " must be less than or equal to m=" << m << dendl;
      err = -EINVAL;
    } else if (k > 12){
      derr << "k=" << k
	   << " must be less than or equal to 12" << dendl;
      err = -EINVAL;
    } else if (k+m > 20){
      derr << "k+m=" << k+m
	   << " must be less than or equal to 20" << dendl;
      err = -EINVAL;
    } else if (k<m){
      derr << "m=" << m
	   << " must be less than or equal to k=" << k << dendl;
      err = -EINVAL;
    }
  }

  if (err) {
    derr << "(k, m, c)=(" << k << ", " << m << ", " << c
	 << ") is not a valid parameter." << dendl;
    return err;
  }

  dout(10) << "(k, m, c) set to " << "(" << k << ", " << m << ", "
	   << c << ")"<< dendl;

  // w
  if (parameters.find("w") == parameters.end()){
    dout(10) << "w default to " << DEFAULT_W << dendl;
    w = DEFAULT_W;
  } else {
    std::string err_w, value_w;
    value_w = parameters.find("w")->second;
    w = strict_strtol(value_w.c_str(), 10, &err_w);

    if (!err_w.empty()){
      derr << "could not convert w=" << value_w << "to int" << dendl;
      dout(10) << "w default to " << DEFAULT_W << dendl;
      w = DEFAULT_W;

    } else if (w != 8 && w != 16 && w != 32) {
      derr << "w=" << w
	   << " must be one of {8, 16, 32}" << dendl;
      dout(10) << "w default to " << DEFAULT_W << dendl;
      w = DEFAULT_W;

    } else {
      dout(10) << "w set to " << w << dendl;
    }
  }
  return 0;
}

void ErasureCodeShecReedSolomonVandermonde::prepare()
{
  // setup shared encoding table
  int** p_enc_table =
    tcache.getEncodingTable(technique, k, m, c, w);

  if (!*p_enc_table) {
    dout(10) << "[ cache tables ] creating coeff for k=" <<
      k << " m=" << m << " c=" << c << " w=" << w << dendl;

    matrix = shec_reedsolomon_coding_matrix(k, m, c, w, technique);

    // either our new created table is stored or if it has been
    // created in the meanwhile the locally allocated table will be
    // freed by setEncodingTable
    matrix = tcache.setEncodingTable(technique, k, m, c, w, matrix);
  } else {
    matrix = *p_enc_table;
  }

  dout(10) << " [ technique ] = " <<
    ((technique == MULTIPLE) ? "multiple" : "single") << dendl;

  assert((technique == SINGLE) || (technique == MULTIPLE));

}
