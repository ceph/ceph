/* reed_sol.c
 * James S. Plank

Jerasure - A C/C++ Library for a Variety of Reed-Solomon and RAID-6 Erasure Coding Techniques

Revision 1.2A
May 24, 2011

James S. Plank
Department of Electrical Engineering and Computer Science
University of Tennessee
Knoxville, TN 37996
plank@cs.utk.edu

Copyright (c) 2011, James S. Plank
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

 - Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.

 - Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in
   the documentation and/or other materials provided with the
   distribution.

 - Neither the name of the University of Tennessee nor the names of its
   contributors may be used to endorse or promote products derived
   from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "galois.h"
#include "jerasure.h"
#include "reed_sol.h"

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

int *reed_sol_r6_coding_matrix(int k, int w)
{
  int *matrix;
  int i, tmp;

  if (w != 8 && w != 16 && w != 32) return NULL;

  matrix = talloc(int, 2*k);
  if (matrix == NULL) return NULL;

  for (i = 0; i < k; i++) matrix[i] = 1;
  matrix[k] = 1;
  tmp = 1;
  for (i = 1; i < k; i++) {
    tmp = galois_single_multiply(tmp, 2, w);
    matrix[k+i] = tmp;
  }
  return matrix;
}

int *reed_sol_vandermonde_coding_matrix(int k, int m, int w)
{
  int i, j;
  int *vdm, *dist;

  vdm = reed_sol_big_vandermonde_distribution_matrix(k+m, k, w);
  if (vdm == NULL) return NULL;
  dist = talloc(int, m*k);
  if (dist == NULL) {
    free(vdm);
    return NULL;
  }

  i = k*k;
  for (j = 0; j < m*k; j++) {
    dist[j] = vdm[i];
    i++;
  }
  free(vdm);
  return dist;
}

static int prim32 = -1;

#define rgw32_mask(v) ((v) & 0x80000000)

void reed_sol_galois_w32_region_multby_2(char *region, int nbytes) 
{
  int *l1;
  int *ltop;
  char *ctop;

  if (prim32 == -1) prim32 = galois_single_multiply((1 << 31), 2, 32);

  ctop = region + nbytes;
  ltop = (int *) ctop;
  l1 = (int *) region;

  while (l1 < ltop) {
    *l1 = ((*l1) << 1) ^ ((*l1 & 0x80000000) ? prim32 : 0);
    l1++;
  }
}

static int prim08 = -1;
static int mask08_1 = -1;
static int mask08_2 = -1;

void reed_sol_galois_w08_region_multby_2(char *region, int nbytes)
{
  unsigned int *l1;
  unsigned int *ltop;
  char *ctop;
  unsigned int tmp, tmp2;
  

  if (prim08 == -1) {
    tmp = galois_single_multiply((1 << 7), 2, 8);
    prim08 = 0;
    while (tmp != 0) {
      prim08 |= tmp;
      tmp = (tmp << 8);
    }
    tmp = (1 << 8) - 2;
    mask08_1 = 0;
    while (tmp != 0) {
      mask08_1 |= tmp;
      tmp = (tmp << 8);
    }
    tmp = (1 << 7);
    mask08_2 = 0;
    while (tmp != 0) {
      mask08_2 |= tmp;
      tmp = (tmp << 8);
    }
  }

  ctop = region + nbytes;
  ltop = (unsigned int *) ctop;
  l1 = (unsigned int *) region;

  while (l1 < ltop) {
    tmp = ((*l1) << 1) & mask08_1;
    tmp2 = (*l1) & mask08_2;
    tmp2 = ((tmp2 << 1) - (tmp2 >> 7));
    *l1 = (tmp ^ (tmp2 & prim08));
    l1++;
  }
}

static int prim16 = -1;
static int mask16_1 = -1;
static int mask16_2 = -1;

void reed_sol_galois_w16_region_multby_2(char *region, int nbytes)
{
  unsigned int *l1;
  unsigned int *ltop;
  char *ctop;
  unsigned int tmp, tmp2;
  

  if (prim16 == -1) {
    tmp = galois_single_multiply((1 << 15), 2, 16);
    prim16 = 0;
    while (tmp != 0) {
      prim16 |= tmp;
      tmp = (tmp << 16);
    }
    tmp = (1 << 16) - 2;
    mask16_1 = 0;
    while (tmp != 0) {
      mask16_1 |= tmp;
      tmp = (tmp << 16);
    }
    tmp = (1 << 15);
    mask16_2 = 0;
    while (tmp != 0) {
      mask16_2 |= tmp;
      tmp = (tmp << 16);
    }
  }

  ctop = region + nbytes;
  ltop = (unsigned int *) ctop;
  l1 = (unsigned int *) region;

  while (l1 < ltop) {
    tmp = ((*l1) << 1) & mask16_1;
    tmp2 = (*l1) & mask16_2;
    tmp2 = ((tmp2 << 1) - (tmp2 >> 15));
    *l1 = (tmp ^ (tmp2 & prim16));
    l1++;
  }
}

int reed_sol_r6_encode(int k, int w, char **data_ptrs, char **coding_ptrs, int size)
{
  int i;

  /* First, put the XOR into coding region 0 */

  memcpy(coding_ptrs[0], data_ptrs[0], size);

  for (i = 1; i < k; i++) galois_region_xor(coding_ptrs[0], data_ptrs[i], coding_ptrs[0], size);

  /* Next, put the sum of (2^j)*Dj into coding region 1 */

  memcpy(coding_ptrs[1], data_ptrs[k-1], size);

  for (i = k-2; i >= 0; i--) {
    switch (w) {
      case 8:  reed_sol_galois_w08_region_multby_2(coding_ptrs[1], size); break;
      case 16: reed_sol_galois_w16_region_multby_2(coding_ptrs[1], size); break;
      case 32: reed_sol_galois_w32_region_multby_2(coding_ptrs[1], size); break;
      default: return 0;
    }

    galois_region_xor(coding_ptrs[1], data_ptrs[i], coding_ptrs[1], size);
  }
  return 1;
}

int *reed_sol_extended_vandermonde_matrix(int rows, int cols, int w)
{
  int *vdm;
  int i, j, k;

  if (w < 30 && (1 << w) < rows) return NULL;
  if (w < 30 && (1 << w) < cols) return NULL;

  vdm = talloc(int, rows*cols);
  if (vdm == NULL) { return NULL; }
  
  vdm[0] = 1;
  for (j = 1; j < cols; j++) vdm[j] = 0;
  if (rows == 1) return vdm;

  i=(rows-1)*cols;
  for (j = 0; j < cols-1; j++) vdm[i+j] = 0;
  vdm[i+j] = 1;
  if (rows == 2) return vdm;

  for (i = 1; i < rows-1; i++) {
    k = 1;
    for (j = 0; j < cols; j++) {
      vdm[i*cols+j] = k;
      k = galois_single_multiply(k, i, w);
    }
  }
  return vdm;
}

int *reed_sol_big_vandermonde_distribution_matrix(int rows, int cols, int w)
{
  int *dist;
  int i, j, k;
  int sindex, srindex, siindex, tmp;

  if (cols >= rows) return NULL;
  
  dist = reed_sol_extended_vandermonde_matrix(rows, cols, w);
  if (dist == NULL) return NULL;

  sindex = 0;
  for (i = 1; i < cols; i++) {
    sindex += cols;

    /* Find an appropriate row -- where i,i != 0 */
    srindex = sindex+i;
    for (j = i; j < rows && dist[srindex] == 0; j++) srindex += cols;
    if (j >= rows) {   /* This should never happen if rows/w are correct */
      fprintf(stderr, "reed_sol_big_vandermonde_distribution_matrix(%d,%d,%d) - couldn't make matrix\n", 
             rows, cols, w);
      exit(1);
    }
 
    /* If necessary, swap rows */
    if (j != i) {
      srindex -= i;
      for (k = 0; k < cols; k++) {
        tmp = dist[srindex+k];
        dist[srindex+k] = dist[sindex+k];
        dist[sindex+k] = tmp;
      }
    }
  
    /* If Element i,i is not equal to 1, multiply the column by 1/i */

    if (dist[sindex+i] != 1) {
      tmp = galois_single_divide(1, dist[sindex+i], w);
      srindex = i;
      for (j = 0; j < rows; j++) {
        dist[srindex] = galois_single_multiply(tmp, dist[srindex], w);
        srindex += cols;
      }
    }
 
    /* Now, for each element in row i that is not in column 1, you need
       to make it zero.  Suppose that this is column j, and the element
       at i,j = e.  Then you want to replace all of column j with 
       (col-j + col-i*e).   Note, that in row i, col-i = 1 and col-j = e.
       So (e + 1e) = 0, which is indeed what we want. */

    for (j = 0; j < cols; j++) {
      tmp = dist[sindex+j];
      if (j != i && tmp != 0) {
        srindex = j;
        siindex = i;
        for (k = 0; k < rows; k++) {
          dist[srindex] = dist[srindex] ^ galois_single_multiply(tmp, dist[siindex], w);
          srindex += cols;
          siindex += cols;
        }
      }
    }
  }
  /* We desire to have row k be all ones.  To do that, multiply
     the entire column j by 1/dist[k,j].  Then row j by 1/dist[j,j]. */

  sindex = cols*cols;
  for (j = 0; j < cols; j++) {
    tmp = dist[sindex];
    if (tmp != 1) { 
      tmp = galois_single_divide(1, tmp, w);
      srindex = sindex;
      for (i = cols; i < rows; i++) {
        dist[srindex] = galois_single_multiply(tmp, dist[srindex], w);
        srindex += cols;
      }
    }
    sindex++;
  }

  /* Finally, we'd like the first column of each row to be all ones.  To
     do that, we multiply the row by the inverse of the first element. */

  sindex = cols*(cols+1);
  for (i = cols+1; i < rows; i++) {
    tmp = dist[sindex];
    if (tmp != 1) { 
      tmp = galois_single_divide(1, tmp, w);
      for (j = 0; j < cols; j++) dist[sindex+j] = galois_single_multiply(dist[sindex+j], tmp, w);
    }
    sindex += cols;
  }

  return dist;
}

