// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *               2013 CERN/Sitzerland
 *
 * Authors: Loic Dachary <loic@dachary.org>
 *          Andreas-Joachim Peters <andreas.joachim.peters@cern.ch> 
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

// -------------------------------------------------------------------------
// constant used in the block alignment function to allow for vector ops
// -------------------------------------------------------------------------
#define LARGEST_VECTOR_WORDSIZE 16

// -------------------------------------------------------------------------
// switch to 128-bit XOR operations if possible 
// -------------------------------------------------------------------------
#if __GNUC__ > 4 || \
  (__GNUC__ == 4 && (__GNUC_MINOR__ > 5) ) || \
  (__clang__ == 1 )
#pragma message "* using 128-bit vector operations in " __FILE__ 
// -------------------------------------------------------------------------
// use 128-bit pointer
// -------------------------------------------------------------------------
typedef long vector_op_t __attribute__ ((vector_size (16)));
#define VECTOR_WORDSIZE 16
#else
// -------------------------------------------------------------------------
// use 64-bit pointer
// -------------------------------------------------------------------------
typedef unsigned long long vector_op_t;
#define VECTOR_WORDSIZE 8
#endif


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

int ErasureCodeJerasure::minimum_to_decode(const set<int> &want_to_read,
                                           const set<int> &available_chunks,
                                           set<int> *minimum) {
  set<int>::iterator i;
  set<int>::iterator o;
  unsigned j;
  if (!lp) {
    // -----------------------------------------------------------------------
    // no local parity
    // -----------------------------------------------------------------------
    if (includes(available_chunks.begin(), available_chunks.end(),
		 want_to_read.begin(), want_to_read.end())) {
      *minimum = want_to_read;
    } else {
      if (available_chunks.size() < (unsigned)k)
	return -EIO;
      set<int>::iterator i;
      unsigned j;
      for (i = available_chunks.begin(), j = 0; j < (unsigned)k; i++, j++)
	minimum->insert(*i);
    }
    return 0;
  } else {
    // -----------------------------------------------------------------------
    // basic pyramid code:local parity
    // -----------------------------------------------------------------------
    int n_sub_chunks = k/lp;
    int lp_recovered=0;

    for (i = want_to_read.begin(); i != want_to_read.end(); ++i) {
      if ( available_chunks.count(*i) ) {
	// -------------------------------------------------------------------
	// if wanted and ok, just add it to the minimum set
	// -------------------------------------------------------------------
	minimum->insert(*i);
      } else {
	// -------------------------------------------------------------------
	// check if only one chunk is missing in a local parity subset
	// -------------------------------------------------------------------
	for (register int s=0; s<lp; s++) {
	  // -----------------------------------------------------------------
	  // which chunks are in this sub group
	  // -----------------------------------------------------------------
	  int n_sub_start = s*n_sub_chunks;
	  int n_sub_stop = (s+1) * n_sub_chunks;
	  if (n_sub_stop>k) n_sub_stop = k;
	  // -----------------------------------------------------------------
	  // check if <i> is in this subgroup
	  // -----------------------------------------------------------------
	  if ( ( *i>=n_sub_start ) && (*i<n_sub_stop) ) {
	    // ---------------------------------------------------------------
	    // this is our subgroup
	    // ---------------------------------------------------------------
	    size_t n_miss=0;
	    // ---------------------------------------------------------------
	    // count the missing chunks
	    // ---------------------------------------------------------------
	    for (int l=n_sub_start; l< n_sub_stop; l++) {
	      if (!available_chunks.count(l)) {
		n_miss++;
	      }
	    }
	    // ---------------------------------------------------------------
	    // if only one is missing, we recover it with the subgroup
	    // ---------------------------------------------------------------
	    if (n_miss==1) {
	      for (int l=n_sub_start; l< n_sub_stop; l++) {
		if (available_chunks.count(l)) {
		  minimum->insert(l);
		}
	      }
	      minimum->insert(k+m+s);
	      lp_recovered++;
	    } else {
	      // -------------------------------------------------------------
	      // if there are more missing we need to add atleast k of (k+m) 
	      // chunks
	      // -------------------------------------------------------------
	      for (o = available_chunks.begin(), j = 0; 
		   j < (unsigned) (k-lp_recovered); 
		   o++, j++) {
		if ( (*o) >= (k+m) ) {
		  // ---------------------------------------------------------
		  // local parity is not usable for erasure recovery
		  // ---------------------------------------------------------
		  return -EIO;
		}
		minimum->insert(*o);
	      }
	    }
	  }
	}
      }
    }
    return 0;
  }
}

int ErasureCodeJerasure::minimum_to_decode_with_cost(const set<int> &want_to_read,
                                                     const map<int, int> &available,
                                                     set<int> *minimum)
{
  set <int> available_chunks;
  for (map<int, int>::const_iterator i = available.begin();
       i != available.end();
       i++)
    available_chunks.insert(i->first);
  return minimum_to_decode(want_to_read, available_chunks, minimum);
}

int ErasureCodeJerasure::encode(const set<int> &want_to_encode,
                                const bufferlist &in,
                                map<int, bufferlist> *encoded)
{
  unsigned alignment = get_alignment();
  unsigned tail = in.length() % alignment;
  unsigned padded_length = in.length() + ( tail ?  ( alignment - tail ) : 0 );
  dout(10) << "encode adjusted buffer length from " << in.length()
	   << " to " << padded_length << dendl;
  assert(padded_length % k == 0);
  unsigned blocksize = padded_length / k;
  unsigned length = blocksize * ( k + m + lp );
  bufferlist out(in);

  bufferptr pad(length - in.length());
  pad.zero(0, padded_length - in.length());
  out.push_back(pad);
  char *chunks[k + m + lp];
  for (int i = 0; i < k + m + lp; i++) {
    bufferlist &chunk = (*encoded)[i];
    chunk.substr_of(out, i * blocksize, blocksize);
    chunks[i] = chunk.c_str();
  }

  bool encode_erasure=false;
  bool encode_lp=false;
  // -------------------------------------------------------------------------
  // check if we need to do erasure encoding and/or local parity encoding
  // -------------------------------------------------------------------------
  for (int j=0; j<(int)want_to_encode.size(); j++) {
    encode_erasure |= (j<k)?true:false;
    encode_lp |= (j>=k)?true:false;
  }

  // -------------------------------------------------------------------------
  // if needed do erasure encoding : jerasure
  // -------------------------------------------------------------------------
  if (encode_erasure) jerasure_encode(&chunks[0], &chunks[k], blocksize);
  // -------------------------------------------------------------------------
  // if needed do local parity encoding : basic pyramid code
  // -------------------------------------------------------------------------
  if (encode_lp && lp) lp_encode(&chunks[0], &chunks[k+m], blocksize);

  for (int i = 0; i < k + m + lp; i++) {
    if (!want_to_encode.count(i))
      encoded->erase(i);
  }
  return 0;
}

int ErasureCodeJerasure::decode(const set<int> &want_to_read,
                                const map<int, bufferlist> &chunks,
                                map<int, bufferlist> *decoded)
{
  unsigned blocksize = (*chunks.begin()).second.length();
  int erasures[k + m + lp + 1];
  int erasures_count = 0;
  char *data[k];
  char *coding[m+lp];
  for (int i =  0; i < k + m + lp; i++) {
    if (chunks.find(i) == chunks.end()) {
      erasures[erasures_count] = i;
      erasures_count++;
      bufferptr ptr(blocksize);
      (*decoded)[i].push_front(ptr);
    } else {
      (*decoded)[i] = chunks.find(i)->second;
    }
    if (i < k)
      data[i] = (*decoded)[i].c_str();
    else
      coding[i - k] = (*decoded)[i].c_str();
  }
  erasures[erasures_count] = -1;

  if (erasures_count > 0) {
    // -----------------------------------------------------------------------
    // try first with local parity : basic pyramid code
    // -----------------------------------------------------------------------
    if (lp) {
      std::set<int>remaining_erasures;
      if (lp_decode(erasures, 
		    data , 
		    coding, 
		    blocksize, 
		    remaining_erasures, 
		    want_to_read))
	return 0;
      // ---------------------------------------------------------------------
      // rewrite the erasures array with the remaining blocks to repair
      // ---------------------------------------------------------------------
      int i=0;
      for (std::set<int>::iterator it=remaining_erasures.begin(); 
	   it!=remaining_erasures.end(); 
	   ++it) {
	erasures[i] = *it;
	i++;
      }
      erasures[i]=-1;
    }
    // -----------------------------------------------------------------------
    // evt. do also erasure deocding if necessary : jerasure
    // -----------------------------------------------------------------------
    return jerasure_decode(erasures, data, coding, blocksize);
  } else {
    return 0;
  }
}

void ErasureCodeJerasure::lp_encode(char **data,
					     char **coding,
					     int blocksize) {
  vector_op_t* dataword[k];
  vector_op_t* codingword[lp];
  for (int i=0; i<k; i++) {
    dataword[i] = (vector_op_t *)data[i];
  }

  for (int i=0; i<lp; i++) {
    codingword[i] = (vector_op_t *)coding[i];
  }
  
  int loop = blocksize / VECTOR_WORDSIZE;
  int n_sub_chunks = k/lp;

  // -------------------------------------------------------------------------
  // compute local parities : basic pyramid code
  // -------------------------------------------------------------------------
  for (register int s=0; s<lp; s++) {
    int n_sub_start = s*n_sub_chunks;
    int n_sub_stop = (s+1) * n_sub_chunks;
    if (n_sub_stop>k) n_sub_stop = k;
    for (register int l = n_sub_start; l<n_sub_stop; l++) {
      vector_op_t* cw=codingword[s];
      bool x_or = true;
      vector_op_t* dw=dataword[l];
      if (l==n_sub_start)
	x_or=false;
      for (register int i=0; i<loop; i++) {
	if (!x_or)
	  *cw++ = *dw++;
	else
	  *cw++ ^= *dw++;
      }      
    }
  }
}

bool ErasureCodeJerasure::lp_decode(int* erasures,
				    char **data,
				    char **coding,
				    int blocksize,
				    std::set<int>&remaining_erasures,
				    const set<int> &want_to_read
				    ) {
  vector_op_t* dataword[k];
  vector_op_t* codingword[lp];

  bool all_recovered=true;

  for (int i=0; i<k;i++) {
    if (erasures[i]>-1) {
      remaining_erasures.insert(erasures[i]);
    } else {
      break;
    }
  }

  // data chunks
  for (int i=0; i< k; i++) {
    dataword[i] = (vector_op_t*)data[i];
  }
  
  // parity chunks
  for (int i=0; i<lp; i++) {
    codingword[i] = (vector_op_t*)coding[m+i];
  }
  
  int loop = blocksize / VECTOR_WORDSIZE;
  int n_sub_chunks = k/lp;
  
  // -------------------------------------------------------------------------
  // decode using a local parity : basic pyramid code
  // -------------------------------------------------------------------------
  for (register int s=0; s<lp; s++) {
    int n_sub_start = s*n_sub_chunks;
    int n_sub_stop = (s+1) * n_sub_chunks;
    if (n_sub_stop>k) n_sub_stop = k;
    // see how many are missing here
    int n_miss=0;
    int reco_chunk=0;
    std::set<int>reco_erasures;

    for (register int l = n_sub_start; l<n_sub_stop; l++) {
      if (remaining_erasures.count(l)) {
	n_miss++;
	reco_chunk=l;	       
      } else {
	reco_erasures.insert(l);
      }
    }

    if (n_miss==1) {
      // ---------------------------------------------------------------------
      // only reconstruct if this chunk is actually wanted
      // -> we can repair this with local parity
      // ---------------------------------------------------------------------
      bool x_or = false;  
      // ---------------------------------------------------------------------
      // XOR all data blocks existing
      // ---------------------------------------------------------------------
      for (std::set<int>::iterator r=reco_erasures.begin(); 
	   r!=reco_erasures.end(); 
	   ++r) {       
	vector_op_t* cw=dataword[reco_chunk];
	if (r!=reco_erasures.begin()) 
	  x_or = true;
	vector_op_t* dw=dataword[*r];
	for (register int i=0; i<loop; i++) {
	  if (!x_or)
	    *cw++ = *dw++;
	  else
	    *cw++ ^= *dw++;
	}      
      }
      vector_op_t* cw=dataword[reco_chunk];
      vector_op_t* dw=codingword[s];
	
      // ---------------------------------------------------------------------
      // XOR the local parity block
      // ---------------------------------------------------------------------
      for (register int i=0; i<loop; i++) {
	*cw++ ^= *dw++;
      }
      remaining_erasures.erase(reco_chunk);
    } 
  }
  // -------------------------------------------------------------------------
  // check if everythhing in want_to_read has been reconstructed 
  // e.g. nothing is left in the remaining_erasures set
  // -------------------------------------------------------------------------
  for (std::set<int>::iterator it=want_to_read.begin(); 
       it!=want_to_read.end(); 
       it++) {
    if (remaining_erasures.count(*it)) {
      all_recovered=false;
      break;
    }
  }
  return all_recovered;
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

unsigned ErasureCodeJerasureReedSolomonVandermonde::get_alignment()
{
  return k*w*LARGEST_VECTOR_WORDSIZE;
}

void ErasureCodeJerasureReedSolomonVandermonde::parse(const map<std::string,std::string> &parameters)
{
  k = to_int("erasure-code-k", parameters, DEFAULT_K);
  m = to_int("erasure-code-m", parameters, DEFAULT_M);
  w = to_int("erasure-code-w", parameters, DEFAULT_W);
  lp = to_int("erasure-code-lp", parameters, DEFAULT_LOCAL_PARITY);

  if (w != 8 && w != 16 && w != 32) {
    derr << "ReedSolomonVandermonde: w=" << w
	 << " must be one of {8, 16, 32} : revert to 8 " << dendl;
    w = 8;
  }
  if (lp > k) {
    lp=DEFAULT_LOCAL_PARITY;
    derr << "lp=" << lp << " must be less than or equal to k=" << k << " : reverting to lp=" << lp <<dendl;
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

unsigned ErasureCodeJerasureReedSolomonRAID6::get_alignment()
{
  return k*w*LARGEST_VECTOR_WORDSIZE;
}

void ErasureCodeJerasureReedSolomonRAID6::parse(const map<std::string,std::string> &parameters)
{
  k = to_int("erasure-code-k", parameters, DEFAULT_K);
  m = 2;
  w = to_int("erasure-code-w", parameters, DEFAULT_W);
  lp = to_int("erasure-code-lp", parameters, DEFAULT_LOCAL_PARITY);

  if (w != 8 && w != 16 && w != 32) {
    derr << "ReedSolomonRAID6: w=" << w
	 << " must be one of {8, 16, 32} : revert to 8 " << dendl;
    w = 8;
  }
  if (lp > k) {
    lp=DEFAULT_LOCAL_PARITY;
    derr << "lp=" << lp << " must be less than or equal to k=" << k << " : reverting to lp=" << lp <<dendl;
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

unsigned ErasureCodeJerasureCauchy::get_alignment()
{
  return k*w*packetsize*LARGEST_VECTOR_WORDSIZE;
}

void ErasureCodeJerasureCauchy::parse(const map<std::string,std::string> &parameters)
{
  k = to_int("erasure-code-k", parameters, DEFAULT_K);
  m = to_int("erasure-code-m", parameters, DEFAULT_M);
  w = to_int("erasure-code-w", parameters, DEFAULT_W);
  packetsize = to_int("erasure-code-packetsize", parameters, DEFAULT_PACKETSIZE);
  lp = to_int("erasure-code-lp", parameters, DEFAULT_LOCAL_PARITY);
  if (lp > k) {
    lp=DEFAULT_LOCAL_PARITY;
    derr << "lp=" << lp << " must be less than or equal to k=" << k << " : reverting to lp=" << lp <<dendl;
  }
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

unsigned ErasureCodeJerasureLiberation::get_alignment()
{
  return k*w*packetsize*LARGEST_VECTOR_WORDSIZE;
}

void ErasureCodeJerasureLiberation::parse(const map<std::string,std::string> &parameters)
{
  k = to_int("erasure-code-k", parameters, DEFAULT_K);
  m = to_int("erasure-code-m", parameters, DEFAULT_M);
  w = to_int("erasure-code-w", parameters, DEFAULT_W);
  packetsize = to_int("erasure-code-packetsize", parameters, DEFAULT_PACKETSIZE);
  lp = to_int("erasure-code-lp", parameters, DEFAULT_LOCAL_PARITY);

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
  if (lp>k) {
    lp=DEFAULT_LOCAL_PARITY;
    derr << "lp=" << lp << " must be less than or equal to k=" << k << " : reverting to lp=" << lp <<dendl;
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
  lp = to_int("erasure-code-lp", parameters, DEFAULT_LOCAL_PARITY);

  bool error = false;
  if (k > w) {
    derr << "k=" << k << " must be less than or equal to w=" << w << dendl;
    error = true;
  }
  if (packetsize == 0) {
    derr << "packetsize=" << packetsize << " must be set" << dendl;
    error = true;
  }
  if (lp > k) {
    lp=DEFAULT_LOCAL_PARITY;
    derr << "lp=" << lp << " must be less than or equal to k=" << k << " : reverting to lp=" << lp <<dendl;
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
