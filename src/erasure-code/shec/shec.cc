// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 FUJITSU LIMITED
 * Copyright (C) 2014, James S. Plank and Kevin Greenan
 *
 * Author: Takanori Nakao <nakao.takanori@jp.fujitsu.com>
 * Author: Takeshi Miyamae <miyamae.takeshi@jp.fujitsu.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

/* Jerasure's authors:

   Revision 2.x - 2014: James S. Plank and Kevin M. Greenan
   Revision 1.2 - 2008: James S. Plank, Scott Simmerman and Catherine D. Schuman.
   Revision 1.0 - 2007: James S. Plank
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>

#include "shec.h"

extern "C"{
#include "jerasure/include/jerasure.h"
#include "jerasure/include/reed_sol.h"

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

extern int calc_determinant(int *matrix, int dim);
}

double shec_calc_recovery_efficiency1(int k, int m1, int m2, int c1, int c2){
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

int *shec_reedsolomon_coding_matrix(int k, int m, int c, int w, int is_single)
{
  int *matrix;
  int rr, cc, start, end;
  int m1, m2, c1, c2, c1_best = -1, m1_best = -1;
  double min_r_e1;

  if (w != 8 && w != 16 && w != 32) return NULL;

  if (!is_single){

    min_r_e1 = 100.0;

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
	  if (r_e1 < min_r_e1){
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

int shec_make_decoding_matrix(bool prepare, int k, int m, int w, int *matrix, int *erased, int *avails, int *decoding_matrix, int *dm_ids, int *minimum)
{
  int i, j, det = 0;
  int ek;
  int *tmpmat = NULL, tmprow[k+m], element, dup, mindup;

  for (i = 0, j = 0, ek = 0; i < k; i++) {
    if (erased[i] == 1) {
      ek++;
    } else {
      dm_ids[j] = i;
      j++;
    }
  }

  tmpmat = talloc(int, k*k);
  if (tmpmat == NULL) { return -1; }
  for (i = 0; i < k-ek; i++) {
    for (j = 0; j < k; j++) tmpmat[i*k+j] = 0;
    tmpmat[i*k+dm_ids[i]] = 1;
  }

  if (ek > m){
    return -1;
  }

  mindup = k+1;
  int minc[ek];
  for (i=0; i<ek; i++){
    minc[i] = -1;
  }
  int p[ek];
  int pp[k+m];
  for (i=0; i<ek; i++){
    pp[i] = 1;
  }
  for (i=ek; i<m; i++){
    pp[i] = 0;
  }

  do {
    i=0;
    for (j=0; j<m; j++){
      if (pp[j]){
	p[i++] = j;
      }
    }

    bool ok = true;
    for (i = 0; i < ek; i++) {
      if (erased[k+p[i]] == 1 || avails[k+p[i]] == 0) ok = false;
      for (j = 0; j < k; j++) {
	element = matrix[(p[i])*k+j];
	if (element != 0) {
	  if (erased[j] == 0 && avails[j] == 0) ok = false;
	}
      }
    }
    if (ok == false) continue;

    for (i = 0; i < k+m; i++) tmprow[i] = 0;
    for (i = 0; i < m; i++) {
      if (erased[k+i] == 1) {
	for (j = 0; j < k; j++) {
	  if (matrix[i*k+j] != 0 && erased[j] == 0) tmprow[j] = 1;
	}
      }
    }
    for (i = 0; i < ek; i++) {
      tmprow[k+p[i]] = 1;
      for (j = 0; j < k; j++) {
	element = matrix[(p[i])*k+j];
	tmpmat[(k-ek+i)*k+j] = element;
	if (element != 0 && erased[j] == 0) tmprow[j] = 1;
      }
    }
    dup = 0;
    for (j = 0; j < k; j++) {
      if (tmprow[j] > 0) dup++;
    }
    if (dup < mindup) {
      det = calc_determinant(tmpmat, k);
      if (det != 0) {
	mindup = dup;
	for (int i=0; i<ek; i++){
	  minc[i] = p[i];
	}
      }
    }
  } while (std::prev_permutation(pp, pp+m));

  if (minc[0] == -1 && mindup == k+1) {
    fprintf(stderr, "shec_make_decoding_matrix(): can't find recover matrix.\n");
    free(tmpmat);
    return -1;
  }

  for (i = 0; i < k+m; i++) minimum[i] = 0;
  for (i = 0; i < m; i++) {
    if (erased[k+i] == 1) {
      for (j = 0; j < k; j++) {
	if (matrix[i*k+j] != 0 && erased[j] == 0) minimum[j] = 1;
      }
    }
  }
  for (i = 0; i < ek; i++) {
    dm_ids[k-ek+i] = k+minc[i];
    minimum[k+minc[i]] = 1;
    for (j = 0; j < k; j++) {
      element = matrix[(minc[i])*k+j];
      tmpmat[(k-ek+i)*k+j] = element;
      if (element != 0 && erased[j] == 0) minimum[j] = 1;
    }
  }

  if (prepare == true) {
    free(tmpmat);
    return 0;
  }

  i = jerasure_invert_matrix(tmpmat, decoding_matrix, k, w);

  free(tmpmat);

  return i;
}

int shec_matrix_decode(int k, int m, int w, int *matrix,
		       int *erased, int *avails, char **data_ptrs, char **coding_ptrs, int size)
{
  int i, edd;
  int *decoding_matrix = NULL, dm_ids[k];
  int minimum[k + m];

  if (w != 8 && w != 16 && w != 32) return -1;

  /* Find the number of data drives failed */

  edd = 0;
  for (i = 0; i < k; i++) {
    if (erased[i]) {
      edd++;
    }
  }

  decoding_matrix = talloc(int, k*k);
  if (decoding_matrix == NULL) { return -1; }

  if (shec_make_decoding_matrix(false, k, m, w, matrix, erased,
				avails, decoding_matrix, dm_ids, minimum) < 0) {
    free(decoding_matrix);
    return -1;
  }

  /* Decode the data drives */

  for (i = 0; edd > 0 && i < k; i++) {
    if (erased[i]) {
      jerasure_matrix_dotprod(k, w, decoding_matrix+(i*k),
			      dm_ids, i, data_ptrs, coding_ptrs, size);
      edd--;
    }
  }

  /* Re-encode any erased coding devices */

  for (i = 0; i < m; i++) {
    if (erased[k+i]) {
      jerasure_matrix_dotprod(k, w, matrix+(i*k), NULL, i+k,
			      data_ptrs, coding_ptrs, size);
    }
  }

  if (decoding_matrix != NULL) free(decoding_matrix);

  return 0;
}
