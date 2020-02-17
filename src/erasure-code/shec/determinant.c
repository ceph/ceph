// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Fujitsu Laboratories
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "jerasure/include/galois.h"

void print_matrix(int *mat, int dim)
{
  int i, j;

  for (i=0; i<dim; i++) {
    for (j=0; j<dim; j++) {
      printf("%d ", mat[i*dim+j]);
    }
    printf("\n");
  }
}

int calc_determinant(int *matrix, int dim)
{
  int i, j, k, *mat, det = 1, coeff_1, coeff_2, *row;

//  print_matrix(matrix, dim);

  mat = (int *)malloc(sizeof(int)*dim*dim);
  if (mat == NULL) {
    printf("mat malloc err\n");
    goto out0;
  }
  memcpy((int *)mat, (int *)matrix, sizeof(int)*dim*dim);

  row = (int *)malloc(sizeof(int)*dim);
  if (row == NULL) {
    printf("row malloc err\n");
    goto out1;
  }

  for (i=0; i<dim; i++) {
    if (mat[i*dim+i] == 0) {
      for (k=i+1; k<dim; k++) {
	if (mat[k*dim+i] != 0) {
	  memcpy((int *)row, (int *)&mat[k*dim], sizeof(int)*dim);
	  memcpy((int *)&mat[k*dim], (int *)&mat[i*dim], sizeof(int)*dim);
	  memcpy((int *)&mat[i*dim], (int *)row, sizeof(int)*dim);
	  break;
	}
      }
      if (k == dim) {
	det = 0;
	goto out2;
      }
    }
    coeff_1 = mat[i*dim+i];
    for (j=i; j<dim; j++) {
      mat[i*dim+j] = galois_single_divide(mat[i*dim+j], coeff_1, 8);
    }
    for (k=i+1; k<dim; k++) {
      if (mat[k*dim+i] != 0) {
	coeff_2 = mat[k*dim+i];
	for (j=i; j<dim; j++) {
	  mat[k*dim+j] = mat[k*dim+j] ^ galois_single_multiply(mat[i*dim+j], coeff_2, 8);
	}
      }
    }
    det = galois_single_multiply(det, coeff_1, 8);
  }
//  print_matrix(mat, dim);

out2:
  free(row);

out1:
  free(mat);

out0:
  return det;
}
