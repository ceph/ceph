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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <algorithm>
#include "common/debug.h"
#include "ErasureCodeShec.h"
#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"
extern "C" {
#include "jerasure/include/jerasure.h"
#include "jerasure/include/galois.h"

extern int calc_determinant(int *matrix, int dim);
extern int* reed_sol_vandermonde_coding_matrix(int k, int m, int w);
}

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

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
    crush.set_rule_mask_max_size(ruleid, get_chunk_count());
    return crush.get_rule_mask_ruleset(ruleid);
  }
}

int ErasureCodeShec::init(ErasureCodeProfile &profile,
			  ostream *ss)
{
  int err = 0;
  err |= to_string("ruleset-root", profile,
		   &ruleset_root,
		   DEFAULT_RULESET_ROOT, ss);
  err |= to_string("ruleset-failure-domain", profile,
		   &ruleset_failure_domain,
		   DEFAULT_RULESET_FAILURE_DOMAIN, ss);
  err |= parse(profile);
  if (err)
    return err;
  prepare();
  ErasureCode::init(profile, ss);
  return err;
}

unsigned int ErasureCodeShec::get_chunk_size(unsigned int object_size) const
{
  unsigned alignment = get_alignment();
  unsigned tail = object_size % alignment;
  unsigned padded_length = object_size + ( tail ?  ( alignment - tail ) : 0 );

  assert(padded_length % k == 0);
  return padded_length / k;
}

int ErasureCodeShec::minimum_to_decode(const set<int> &want_to_read,
				       const set<int> &available_chunks,
				       set<int> *minimum_chunks)
{
  if (!minimum_chunks) return -EINVAL;

  for (set<int>::iterator it = available_chunks.begin(); it != available_chunks.end(); ++it){
    if (*it < 0 || k+m <= *it) return -EINVAL;
  }

  for (set<int>::iterator it = want_to_read.begin(); it != want_to_read.end(); ++it){
    if (*it < 0 || k+m <= *it) return -EINVAL;
  }

  int want[k + m];
  int avails[k + m];
  int minimum[k + m];

  memset(want, 0, sizeof(want));
  memset(avails, 0, sizeof(avails));
  memset(minimum, 0, sizeof(minimum));
  (*minimum_chunks).clear();

  for (set<int>::const_iterator i = want_to_read.begin();
       i != want_to_read.end();
       ++i) {
    want[*i] = 1;
  }

  for (set<int>::const_iterator i = available_chunks.begin();
       i != available_chunks.end();
       ++i) {
    avails[*i] = 1;
  }

  {
    int decoding_matrix[k*k];
    int dm_row[k];
    int dm_column[k];
    memset(decoding_matrix, 0, sizeof(decoding_matrix));
    memset(dm_row, 0, sizeof(dm_row));
    memset(dm_column, 0, sizeof(dm_column));
    if (shec_make_decoding_matrix(true, want, avails, decoding_matrix, dm_row, dm_column, minimum) < 0) {
      return -EIO;
    }
  }

  for (int i = 0; i < k + m; i++) {
    if (minimum[i] == 1) minimum_chunks->insert(i);
  }

  return 0;
}

int ErasureCodeShec::minimum_to_decode_with_cost(const set<int> &want_to_read,
						 const map<int, int> &available,
						 set<int> *minimum_chunks)
{
  set <int> available_chunks;

  for (map<int, int>::const_iterator i = available.begin();
       i != available.end();
       ++i)
    available_chunks.insert(i->first);

  return minimum_to_decode(want_to_read, available_chunks, minimum_chunks);
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
  return shec_matrix_decode(erased, avails, data, coding, blocksize);
}

unsigned ErasureCodeShecReedSolomonVandermonde::get_alignment() const
{
  return k*w*sizeof(int);
}

int ErasureCodeShecReedSolomonVandermonde::parse(const ErasureCodeProfile &profile)
{
  int err = 0;
  // k, m, c
  if (profile.find("k") == profile.end() &&
      profile.find("m") == profile.end() &&
      profile.find("c") == profile.end()){
    dout(10) << "(k, m, c) default to " << "(" << DEFAULT_K
	     << ", " << DEFAULT_M << ", " << DEFAULT_C << ")" << dendl;
    k = DEFAULT_K; m = DEFAULT_M; c = DEFAULT_C;
  } else if (profile.find("k") == profile.end() ||
	     profile.find("m") == profile.end() ||
	     profile.find("c") == profile.end()){
    dout(10) << "(k, m, c) must be choosed" << dendl;
    err = -EINVAL;
  } else {
    std::string err_k, err_m, err_c, value_k, value_m, value_c;
    value_k = profile.find("k")->second;
    value_m = profile.find("m")->second;
    value_c = profile.find("c")->second;
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
  if (profile.find("w") == profile.end()){
    dout(10) << "w default to " << DEFAULT_W << dendl;
    w = DEFAULT_W;
  } else {
    std::string err_w, value_w;
    value_w = profile.find("w")->second;
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

    matrix = shec_reedsolomon_coding_matrix(technique);

    // either our new created table is stored or if it has been
    // created in the meanwhile the locally allocated table will be
    // freed by setEncodingTable
    matrix = tcache.setEncodingTable(technique, k, m, c, w, matrix);

    dout(10) << "matrix = " << dendl;
    for (int i=0; i<m; i++) {
      char mat[k+1];
      for (int j=0; j<k; j++) {
        if (matrix[i*k+j] > 0) {
          mat[j] = '1';
        } else {
          mat[j] = '0';
        }
      }
      mat[k] = '\0';
      dout(10) << mat << dendl;
    }
  } else {
    matrix = *p_enc_table;
  }

  dout(10) << " [ technique ] = " <<
    ((technique == MULTIPLE) ? "multiple" : "single") << dendl;

  assert((technique == SINGLE) || (technique == MULTIPLE));

}

// ErasureCodeShec::
// Mearged from shec.cc.

double ErasureCodeShec::shec_calc_recovery_efficiency1(int k, int m1, int m2, int c1, int c2){
  int r_eff_k[k];
  double r_e1;
  int i, rr, cc, start, end;
  int first_flag;

  if (m1 < c1 || m2 < c2) return -1;
  if ((m1 == 0 && c1 != 0) || (m2 == 0 && c2 != 0)) return -1;

  for (i=0; i<k; i++) r_eff_k[i] = 100000000;
  r_e1 = 0;

  for (rr=0; rr<m1; rr++){
    start = ((rr*k)/m1) % k;
    end = (((rr+c1)*k)/m1) % k;
    for (cc=start, first_flag=1; first_flag || cc!=end; cc=(cc+1)%k){
      first_flag = 0;
      r_eff_k[cc] = std::min(r_eff_k[cc], ((rr+c1)*k)/m1 - (rr*k)/m1);
    }
    r_e1 += ((rr+c1)*k)/m1 - (rr*k)/m1;
  }

  for (rr=0; rr<m2; rr++){
    start = ((rr*k)/m2) % k;
    end = (((rr+c2)*k)/m2) % k;
    for (cc=start, first_flag=1; first_flag || cc!=end; cc=(cc+1)%k){
      first_flag = 0;
      r_eff_k[cc] = std::min(r_eff_k[cc], ((rr+c2)*k)/m2 - (rr*k)/m2);
    }
    r_e1 += ((rr+c2)*k)/m2 - (rr*k)/m2;
  }

  for (i=0; i<k; i++){
    r_e1 += r_eff_k[i];
  }

  r_e1 /= (k+m1+m2);

  return r_e1;
}

int* ErasureCodeShec::shec_reedsolomon_coding_matrix(int is_single)
{
  int *matrix;
  int rr, cc, start, end;
  int m1, m2, c1, c2;

  if (w != 8 && w != 16 && w != 32) return NULL;

  if (!is_single){
    int c1_best = -1, m1_best = -1;
    double min_r_e1 = 100.0;

    // create all multiple shec pattern and choose best.

    for (c1=0; c1 <= c/2; c1++){
      for (m1=0; m1 <= m; m1++){
        c2 = c-c1;
        m2 = m-m1;

        if (m1 < c1 || m2 < c2) continue;
        if ((m1 == 0 && c1 != 0) || (m2 == 0 && c2 != 0)) continue;
        if ((m1 != 0 && c1 == 0) || (m2 != 0 && c2 == 0)) continue;

        // minimize r_e1

        if (true) {
          double r_e1;
          r_e1 = shec_calc_recovery_efficiency1(k, m1, m2, c1, c2);
          if (min_r_e1 - r_e1 > std::numeric_limits<double>::epsilon() &&
	      r_e1 < min_r_e1) {
            min_r_e1 = r_e1;
            c1_best = c1;
            m1_best = m1;
          }
        }
      }
    }
    m1 = m1_best;
    c1 = c1_best;
    m2 = m - m1_best;
    c2 = c - c1_best;
  } else {
    m1 = 0;
    c1 = 0;
    m2 = m;
    c2 = c;
  }

  // create matrix
  matrix = reed_sol_vandermonde_coding_matrix(k, m, w);

  for (rr=0; rr<m1; rr++){
    end = ((rr*k)/m1) % k;
    start = (((rr+c1)*k)/m1) % k;
    for (cc=start; cc!=end; cc=(cc+1)%k){
      matrix[cc + rr*k] = 0;
    }
  }

  for (rr=0; rr<m2; rr++){
    end = ((rr*k)/m2) % k;
    start = (((rr+c2)*k)/m2) % k;
    for (cc=start; cc!=end; cc=(cc+1)%k){
      matrix[cc + (rr+m1)*k] = 0;
    }
  }

  return matrix;
}

int ErasureCodeShec::shec_make_decoding_matrix(bool prepare, int *want_, int *avails,
                                               int *decoding_matrix, int *dm_row, int *dm_column,
                                               int *minimum)
{
  int mindup = k+1, minp = k+1;
  int want[k + m];
  for (int i = 0; i < k + m; ++i) {
    want[i] = want_[i];
  }

  for (int i = 0; i < m; ++i) {
    if (want[i + k] && !avails[i + k]) {
      for (int j=0; j < k; ++j) {
        if (matrix[i * k + j] > 0) {
          want[j] = 1;
        }
      }
    }
  }

  if (tcache.getDecodingTableFromCache(decoding_matrix,
                                       dm_row, dm_column, minimum,
                                       technique,
                                       k, m, c, w,
                                       want, avails)) {
    return 0;
  }

  for (unsigned long long pp = 0; pp < (1ull << m); ++pp) {

    // select parity chunks
    int ek = 0;
    int p[m];
    for (int i=0; i < m; ++i) {
      if (pp & (1ull << i)) {
        p[ek++] = i;
      }
    }
    if (ek > minp) {
      continue;
    }

    // Are selected parity chunks avail?
    bool ok = true;
    for (int i = 0; i < ek && ok; i++) {
      if (!avails[k+p[i]]) {
        ok = false;
        break;
      }
    }

    if (!ok) {
      continue;
    }

    int tmprow[k + m];
    int tmpcolumn[k];
    for (int i = 0; i < k + m; i++) {
      tmprow[i] = 0;
    }
    for (int i = 0; i < k; i++) {
      tmpcolumn[i] = 0;
    }

    for (int i=0; i < k; i++) {
      if (want[i] && !avails[i]) {
        tmpcolumn[i] = 1;
      }
    }

    // Parity chunks which are used to recovery erased data chunks, are added to tmprow.
    for (int i = 0; i < ek; i++) {
      tmprow[k + p[i]] = 1;
      for (int j = 0; j < k; j++) {
        int element = matrix[(p[i]) * k + j];
        if (element != 0) {
          tmpcolumn[j] = 1;
        }
        if (element != 0 && avails[j] == 1) {
          tmprow[j] = 1;
        }
      }
    }

    int dup_row = 0, dup_column = 0, dup = 0;
    for (int i = 0; i < k + m; i++) {
      if (tmprow[i]) {
        dup_row++;
      }
    }

    for (int i = 0; i < k; i++) {
      if (tmpcolumn[i]) {
        dup_column++;
      }
    }

    if (dup_row != dup_column) {
      continue;
    }
    dup = dup_row;
    if (dup == 0) {
      mindup = dup;
      for (int i = 0; i < k; i++) {
        dm_row[i] = -1;
      }
      for (int i = 0; i < k; i++) {
        dm_column[i] = -1;
      }
      break;
    }

    // minimum is updated.
    if (dup < mindup) {
      int tmpmat[dup * dup];
      {
        for (int i = 0, row = 0; i < k + m; i++) {
          if (tmprow[i]) {
            for (int j = 0, column = 0; j < k; j++) {
              if (tmpcolumn[j]) {
                if (i < k) {
                  tmpmat[row * dup + column] = (i == j ? 1 : 0);
                } else {
                  tmpmat[row * dup + column] = matrix[(i - k) * k + j];
                }
                column++;
              }
            }
            row++;
          }
        }
      }
      int det = calc_determinant(tmpmat, dup);

      if (det != 0) {
        int row_id = 0;
        int column_id = 0;
        for (int i = 0; i < k; i++) {
          dm_row[i] = -1;
        }
        for (int i = 0; i < k; i++) {
          dm_column[i] = -1;
        }

        mindup = dup;
        for (int i=0; i < k + m; i++) {
          if (tmprow[i]) {
            dm_row[row_id++] = i;
          }
        }
        for (int i=0; i < k; i++) {
          if (tmpcolumn[i]) {
            dm_column[column_id++] = i;
          }
        }
        minp = ek;
      }
    }
  }


  if (mindup == k+1) {
    fprintf(stderr, "shec_make_decoding_matrix(): can't find recover matrix.\n");
    return -1;
  }

  for (int i = 0; i < k + m; i++) {
    minimum[i] = 0;
  }

  for (int i=0; i < k && dm_row[i] != -1; i++) {
    minimum[dm_row[i]] = 1;
  }

  for (int i = 0; i < k; ++i) {
    if (want[i] && avails[i]) {
      minimum[i] = 1;
    }
  }

  for (int i = 0; i < m; ++i) {
    if (want[k + i] && avails[k + i] && !minimum[k + i]) {
      for (int j = 0; j < k; ++j) {
        if (matrix[i * k + j] > 0 && !want[j]) {
          minimum[k + i] = 1;
          break;
        }
      }
    }
  }

  if (mindup == 0) {
    return 0;
  }

  int tmpmat[mindup * mindup];
  for (int i=0; i < mindup; i++) {
    for (int j=0; j < mindup; j++) {
      if (dm_row[i] < k) {
        tmpmat[i * mindup + j] = (dm_row[i] == dm_column[j] ? 1 : 0);
      } else {
        tmpmat[i * mindup + j] = matrix[(dm_row[i] - k) * k + dm_column[j]];
      }
    }
    if (dm_row[i] < k) {
      for (int j = 0; j < mindup; j++) {
        if (dm_row[i] == dm_column[j]) {
          dm_row[i] = j;
        }
      }
    } else {
      dm_row[i] -= (k - mindup);
    }
  }

  if (prepare) {
    return 0;
  }

  int ret = jerasure_invert_matrix(tmpmat, decoding_matrix, mindup, w);

  tcache.putDecodingTableToCache(decoding_matrix, dm_row, dm_column, minimum, technique,
                                 k, m, c, w, want, avails);

  return ret;
}

int ErasureCodeShec::shec_matrix_decode(int *want, int *avails, char **data_ptrs,
                                        char **coding_ptrs, int size)
{
  int decoding_matrix[k*k];
  int dm_row[k], dm_column[k];
  int minimum[k + m];

  memset(decoding_matrix, 0, sizeof(decoding_matrix));
  memset(dm_row, -1, sizeof(dm_row));
  memset(dm_column, -1, sizeof(dm_column));
  memset(minimum, -1, sizeof(minimum));

  if (w != 8 && w != 16 && w != 32) return -1;

  if (shec_make_decoding_matrix(false, want, avails, decoding_matrix,
                                dm_row, dm_column, minimum) < 0) {
    return -1;
  }

  // Get decoding matrix size
  int dm_size = 0;
  for (int i = 0; i < k; i++) {
    if (dm_row[i] == -1) {
      break;
    }
    dm_size++;
  }

  char *dm_data_ptrs[dm_size];
  for (int i = 0; i < dm_size; i++) {
    dm_data_ptrs[i] = data_ptrs[dm_column[i]];
  }

  // Decode the data drives
  for (int i = 0; i < dm_size; i++) {
    if (!avails[dm_column[i]]) {
      jerasure_matrix_dotprod(dm_size, w, decoding_matrix + (i * dm_size),
                              dm_row, i, dm_data_ptrs, coding_ptrs, size);
    }
  }

  // Re-encode any erased coding devices
  for (int i = 0; i < m; i++) {
    if (want[k+i] && !avails[k+i]) {
      jerasure_matrix_dotprod(k, w, matrix + (i * k), NULL, i+k,
                              data_ptrs, coding_ptrs, size);
    }
  }

  return 0;
}
