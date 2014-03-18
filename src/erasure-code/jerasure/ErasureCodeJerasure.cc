// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
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

#include <errno.h>
#include <algorithm>
#include "common/debug.h"
#include "ErasureCodeJerasure.h"
#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"
extern "C" {
#include "jerasure.h"
#include "reed_sol.h"
#include "galois.h"
#include "cauchy.h"
#include "liberation.h"
}

// FIXME(loic) this may be too conservative, check back with feedback from Andreas 
#define LARGEST_VECTOR_WORDSIZE 16

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodeJerasure: ";
}

int ErasureCodeJerasure::create_ruleset(const string &name,
					CrushWrapper &crush,
					ostream *ss) const
{
  return crush.add_simple_ruleset(name, ruleset_root, ruleset_failure_domain,
				  "indep", pg_pool_t::TYPE_ERASURE, ss);
}

void ErasureCodeJerasure::init(const map<string,string> &parameters)
{
  dout(10) << "technique=" << technique << dendl;
  map<string,string>::const_iterator parameter;
  parameter = parameters.find("ruleset-root");
  if (parameter != parameters.end())
    ruleset_root = parameter->second;
  parameter = parameters.find("ruleset-failure-domain");
  if (parameter != parameters.end())
    ruleset_failure_domain = parameter->second;
  parse(parameters);
  prepare();
}

unsigned int ErasureCodeJerasure::get_chunk_size(unsigned int object_size) const
{
  unsigned alignment = get_alignment();
  unsigned tail = object_size % alignment;
  unsigned padded_length = object_size + ( tail ?  ( alignment - tail ) : 0 );
  assert(padded_length % k == 0);
  return padded_length / k;
}

int ErasureCodeJerasure::minimum_to_decode(const set<int> &want_to_read,
                                           const set<int> &available_chunks,
                                           set<int> *minimum) 
{
  if (includes(available_chunks.begin(), available_chunks.end(),
	       want_to_read.begin(), want_to_read.end())) {
    *minimum = want_to_read;
  } else {
    if (available_chunks.size() < (unsigned)k)
      return -EIO;
    set<int>::iterator i;
    unsigned j;
    for (i = available_chunks.begin(), j = 0; j < (unsigned)k; ++i, j++)
      minimum->insert(*i);
  }
  return 0;
}

int ErasureCodeJerasure::minimum_to_decode_with_cost(const set<int> &want_to_read,
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

int ErasureCodeJerasure::encode(const set<int> &want_to_encode,
                                const bufferlist &in,
                                map<int, bufferlist> *encoded)
{
  unsigned blocksize = get_chunk_size(in.length());
  unsigned padded_length = blocksize * k;
  dout(10) << "encode adjusted buffer length from " << in.length()
	   << " to " << padded_length << dendl;
  assert(padded_length % k == 0);
  bufferlist out(in);
  if (padded_length - in.length() > 0) {
    bufferptr pad(padded_length - in.length());
    pad.zero();
    out.push_back(pad);
  }
  unsigned coding_length = blocksize * m;
  bufferptr coding(buffer::create_page_aligned(coding_length));
  out.push_back(coding);
  out.rebuild_page_aligned();
  char *chunks[k + m];
  for (int i = 0; i < k + m; i++) {
    bufferlist &chunk = (*encoded)[i];
    chunk.substr_of(out, i * blocksize, blocksize);
    chunks[i] = chunk.c_str();
  }
  jerasure_encode(&chunks[0], &chunks[k], blocksize);
  for (int i = 0; i < k + m; i++) {
    if (want_to_encode.count(i) == 0)
      encoded->erase(i);
  }
  return 0;
}

int ErasureCodeJerasure::decode(const set<int> &want_to_read,
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
  unsigned blocksize = (*chunks.begin()).second.length();
  int erasures[k + m + 1];
  int erasures_count = 0;
  char *data[k];
  char *coding[m];
  for (int i =  0; i < k + m; i++) {
    if (chunks.find(i) == chunks.end()) {
      erasures[erasures_count] = i;
      erasures_count++;
      bufferptr ptr(buffer::create_page_aligned(blocksize));
      (*decoded)[i].push_front(ptr);
    } else {
      (*decoded)[i] = chunks.find(i)->second;
      (*decoded)[i].rebuild_page_aligned();
    }
    if (i < k)
      data[i] = (*decoded)[i].c_str();
    else
      coding[i - k] = (*decoded)[i].c_str();
  }
  erasures[erasures_count] = -1;

  if (erasures_count > 0)
    return jerasure_decode(erasures, data, coding, blocksize);
  else
    return 0;
}

int ErasureCodeJerasure::to_int(const std::string &name,
                                const map<std::string,std::string> &parameters,
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

bool ErasureCodeJerasure::is_prime(int value)
{
  int prime55[] = {
    2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,
    73,79,83,89,97,101,103,107,109,113,127,131,137,139,149,
    151,157,163,167,173,179,
    181,191,193,197,199,211,223,227,229,233,239,241,251,257
  };
  int i;
  for (i = 0; i < 55; i++)
    if (value == prime55[i])
      return true;
  return false;
}

// 
// ErasureCodeJerasureReedSolomonVandermonde
//
void ErasureCodeJerasureReedSolomonVandermonde::jerasure_encode(char **data,
                                                                char **coding,
                                                                int blocksize)
{
  jerasure_matrix_encode(k, m, w, matrix, data, coding, blocksize);
}

int ErasureCodeJerasureReedSolomonVandermonde::jerasure_decode(int *erasures,
                                                                char **data,
                                                                char **coding,
                                                                int blocksize)
{
  return jerasure_matrix_decode(k, m, w, matrix, 1,
				erasures, data, coding, blocksize);
}

unsigned ErasureCodeJerasureReedSolomonVandermonde::get_alignment() const
{
  unsigned alignment = k*w*sizeof(int);
  if ( ((w*sizeof(int))%LARGEST_VECTOR_WORDSIZE) )
    alignment = k*w*LARGEST_VECTOR_WORDSIZE;
  return alignment;

}

void ErasureCodeJerasureReedSolomonVandermonde::parse(const map<std::string,std::string> &parameters)
{
  k = to_int("k", parameters, DEFAULT_K);
  m = to_int("m", parameters, DEFAULT_M);
  w = to_int("w", parameters, DEFAULT_W);
  if (w != 8 && w != 16 && w != 32) {
    derr << "ReedSolomonVandermonde: w=" << w
	 << " must be one of {8, 16, 32} : revert to 8 " << dendl;
    w = 8;
  }
}

void ErasureCodeJerasureReedSolomonVandermonde::prepare()
{
  matrix = reed_sol_vandermonde_coding_matrix(k, m, w);
}

// 
// ErasureCodeJerasureReedSolomonRAID6
//
void ErasureCodeJerasureReedSolomonRAID6::jerasure_encode(char **data,
                                                                char **coding,
                                                                int blocksize)
{
  reed_sol_r6_encode(k, w, data, coding, blocksize);
}

int ErasureCodeJerasureReedSolomonRAID6::jerasure_decode(int *erasures,
							 char **data,
							 char **coding,
							 int blocksize)
{
  return jerasure_matrix_decode(k, m, w, matrix, 1, erasures, data, coding, blocksize);
}

unsigned ErasureCodeJerasureReedSolomonRAID6::get_alignment() const
{
  unsigned alignment = k*w*sizeof(int);
  if ( ((w*sizeof(int))%LARGEST_VECTOR_WORDSIZE) )
    alignment = k*w*LARGEST_VECTOR_WORDSIZE;
  return alignment;
}

void ErasureCodeJerasureReedSolomonRAID6::parse(const map<std::string,std::string> &parameters)
{
  k = to_int("k", parameters, DEFAULT_K);
  m = 2;
  w = to_int("w", parameters, DEFAULT_W);
  if (w != 8 && w != 16 && w != 32) {
    derr << "ReedSolomonRAID6: w=" << w
	 << " must be one of {8, 16, 32} : revert to 8 " << dendl;
    w = 8;
  }
}

void ErasureCodeJerasureReedSolomonRAID6::prepare()
{
  matrix = reed_sol_r6_coding_matrix(k, w);
}

// 
// ErasureCodeJerasureCauchy
//
void ErasureCodeJerasureCauchy::jerasure_encode(char **data,
						char **coding,
						int blocksize)
{
  jerasure_schedule_encode(k, m, w, schedule,
			   data, coding, blocksize, packetsize);
}

int ErasureCodeJerasureCauchy::jerasure_decode(int *erasures,
					       char **data,
					       char **coding,
					       int blocksize)
{
  return jerasure_schedule_decode_lazy(k, m, w, bitmatrix,
				       erasures, data, coding, blocksize, packetsize, 1);
}

unsigned ErasureCodeJerasureCauchy::get_alignment() const
{
  unsigned alignment = k*w*packetsize*sizeof(int);
  if ( ((w*packetsize*sizeof(int))%LARGEST_VECTOR_WORDSIZE) )
    alignment = k*w*packetsize*LARGEST_VECTOR_WORDSIZE;
  return alignment;
}

void ErasureCodeJerasureCauchy::parse(const map<std::string,std::string> &parameters)
{
  k = to_int("k", parameters, DEFAULT_K);
  m = to_int("m", parameters, DEFAULT_M);
  w = to_int("w", parameters, DEFAULT_W);
  packetsize = to_int("packetsize", parameters, DEFAULT_PACKETSIZE);
}

void ErasureCodeJerasureCauchy::prepare_schedule(int *matrix)
{
  bitmatrix = jerasure_matrix_to_bitmatrix(k, m, w, matrix);
  schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
}

// 
// ErasureCodeJerasureCauchyOrig
//
void ErasureCodeJerasureCauchyOrig::prepare()
{
  int *matrix = cauchy_original_coding_matrix(k, m, w);
  prepare_schedule(matrix);
  free(matrix);
}

// 
// ErasureCodeJerasureCauchyGood
//
void ErasureCodeJerasureCauchyGood::prepare()
{
  int *matrix = cauchy_good_general_coding_matrix(k, m, w);
  prepare_schedule(matrix);
  free(matrix);
}

// 
// ErasureCodeJerasureLiberation
//
ErasureCodeJerasureLiberation::~ErasureCodeJerasureLiberation()
{
  if (bitmatrix)
    free(bitmatrix);
  if (schedule)
    jerasure_free_schedule(schedule);
}

void ErasureCodeJerasureLiberation::jerasure_encode(char **data,
                                                    char **coding,
                                                    int blocksize)
{
  jerasure_schedule_encode(k, m, w, schedule, data,
			   coding, blocksize, packetsize);
}

int ErasureCodeJerasureLiberation::jerasure_decode(int *erasures,
                                                    char **data,
                                                    char **coding,
                                                    int blocksize)
{
  return jerasure_schedule_decode_lazy(k, m, w, bitmatrix, erasures, data,
				       coding, blocksize, packetsize, 1);
}

unsigned ErasureCodeJerasureLiberation::get_alignment() const
{
  unsigned alignment = k*w*packetsize*sizeof(int);
  if ( ((w*packetsize*sizeof(int))%LARGEST_VECTOR_WORDSIZE) )
    alignment = k*w*packetsize*LARGEST_VECTOR_WORDSIZE;
  return alignment;
}

void ErasureCodeJerasureLiberation::parse(const map<std::string,std::string> &parameters)
{
  k = to_int("k", parameters, DEFAULT_K);
  m = to_int("m", parameters, DEFAULT_M);
  w = to_int("w", parameters, DEFAULT_W);
  packetsize = to_int("packetsize", parameters, DEFAULT_PACKETSIZE);

  bool error = false;
  if (k > w) {
    derr << "k=" << k << " must be less than or equal to w=" << w << dendl;
    error = true;
  }
  if (w <= 2 || !is_prime(w)) {
    derr <<  "w=" << w << " must be greater than two and be prime" << dendl;
    error = true;
  }
  if (packetsize == 0) {
    derr << "packetsize=" << packetsize << " must be set" << dendl;
    error = true;
  }
  if ((packetsize%(sizeof(int))) != 0) {
    derr << "packetsize=" << packetsize
	 << " must be a multiple of sizeof(int) = " << sizeof(int) << dendl;
    error = true;
  }
  if (error) {
    derr << "reverting to k=" << DEFAULT_K << ", w="
	 << DEFAULT_W << ", packetsize=" << DEFAULT_PACKETSIZE << dendl;
    k = DEFAULT_K;
    w = DEFAULT_W;
    packetsize = DEFAULT_PACKETSIZE;
  }
}

void ErasureCodeJerasureLiberation::prepare()
{
  bitmatrix = liberation_coding_bitmatrix(k, w);
  schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
}

// 
// ErasureCodeJerasureBlaumRoth
//
void ErasureCodeJerasureBlaumRoth::prepare()
{
  bitmatrix = blaum_roth_coding_bitmatrix(k, w);
  schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
}

// 
// ErasureCodeJerasureLiber8tion
//
void ErasureCodeJerasureLiber8tion::parse(const map<std::string,std::string> &parameters)
{
  k = to_int("k", parameters, DEFAULT_K);
  m = DEFAULT_M;
  w = DEFAULT_W;
  packetsize = to_int("packetsize", parameters, DEFAULT_PACKETSIZE);

  bool error = false;
  if (k > w) {
    derr << "k=" << k << " must be less than or equal to w=" << w << dendl;
    error = true;
  }
  if (packetsize == 0) {
    derr << "packetsize=" << packetsize << " must be set" << dendl;
    error = true;
  }
  if (error) {
    derr << "reverting to k=" << DEFAULT_K << ", packetsize="
	 << DEFAULT_PACKETSIZE << dendl;
    k = DEFAULT_K;
    packetsize = DEFAULT_PACKETSIZE;
  }
}

void ErasureCodeJerasureLiber8tion::prepare()
{
  bitmatrix = liber8tion_coding_bitmatrix(k);
  schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
}
