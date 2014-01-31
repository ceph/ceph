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

#include <errno.h>
#include <algorithm>
#include "common/debug.h"
#include "ErasureCodeJerasure.h"
#include "vectorop.h"
extern "C" {
#include "jerasure.h"
#include "reed_sol.h"
#include "galois.h"
#include "cauchy.h"
#include "liberation.h"
}

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodeJerasure: ";
}

void ErasureCodeJerasure::init(const map<std::string,std::string> &parameters)
{
  dout(10) << "technique=" << technique << dendl;
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

int ErasureCodeJerasure::encode_chunks(vector<bufferlist> &chunks)
{
  assert(chunks.size() == (unsigned int)(k + m));
  char *buffers[k + m];
  unsigned int i = 0;
  for (vector<bufferlist>::iterator chunk = chunks.begin();
       chunk != chunks.end();
       chunk++, i++)
    buffers[i] = chunk->c_str();
  jerasure_encode(&buffers[0], &buffers[k], chunks.front().length());
  return 0;
}

int ErasureCodeJerasure::decode_chunks(vector<bool> erasures,
				       vector<bufferlist> &chunks)
{
  int e[k + m + 1];
  int e_count = 0;

  char *data[k];
  char *coding[m];

  for (unsigned int i = 0; i < chunks.size(); i++) {
    if (i < (unsigned int)k)
      data[i] = chunks[i].c_str();
    else
      coding[i - k] = chunks[i].c_str();
    if (erasures[i])
      e[e_count++] = i;
  }
  e[e_count] = -1;
  if (e_count > 0)
    return jerasure_decode(e, data, coding, chunks.front().length());
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
  k = to_int("erasure-code-k", parameters, DEFAULT_K);
  m = to_int("erasure-code-m", parameters, DEFAULT_M);
  w = to_int("erasure-code-w", parameters, DEFAULT_W);
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
  k = to_int("erasure-code-k", parameters, DEFAULT_K);
  m = 2;
  w = to_int("erasure-code-w", parameters, DEFAULT_W);
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
  k = to_int("erasure-code-k", parameters, DEFAULT_K);
  m = to_int("erasure-code-m", parameters, DEFAULT_M);
  w = to_int("erasure-code-w", parameters, DEFAULT_W);
  packetsize = to_int("erasure-code-packetsize", parameters, DEFAULT_PACKETSIZE);
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
  k = to_int("erasure-code-k", parameters, DEFAULT_K);
  m = to_int("erasure-code-m", parameters, DEFAULT_M);
  w = to_int("erasure-code-w", parameters, DEFAULT_W);
  packetsize = to_int("erasure-code-packetsize", parameters, DEFAULT_PACKETSIZE);

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
  k = to_int("erasure-code-k", parameters, DEFAULT_K);
  m = DEFAULT_M;
  w = DEFAULT_W;
  packetsize = to_int("erasure-code-packetsize", parameters, DEFAULT_PACKETSIZE);

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
