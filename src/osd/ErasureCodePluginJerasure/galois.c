/* Galois.c
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

#define NONE (10)
#define TABLE (11)
#define SHIFT (12)
#define LOGS (13)
#define SPLITW8 (14)

static int prim_poly[33] = 
{ 0, 
/*  1 */     1, 
/*  2 */    07,
/*  3 */    013,
/*  4 */    023,
/*  5 */    045,
/*  6 */    0103,
/*  7 */    0211,
/*  8 */    0435,
/*  9 */    01021,
/* 10 */    02011,
/* 11 */    04005,
/* 12 */    010123,
/* 13 */    020033,
/* 14 */    042103,
/* 15 */    0100003,
/* 16 */    0210013,
/* 17 */    0400011,
/* 18 */    01000201,
/* 19 */    02000047,
/* 20 */    04000011,
/* 21 */    010000005,
/* 22 */    020000003,
/* 23 */    040000041,
/* 24 */    0100000207,
/* 25 */    0200000011,
/* 26 */    0400000107,
/* 27 */    01000000047,
/* 28 */    02000000011,
/* 29 */    04000000005,
/* 30 */    010040000007,
/* 31 */    020000000011, 
/* 32 */    00020000007 };  /* Really 40020000007, but we're omitting the high order bit */

static int mult_type[33] = 
{ NONE, 
/*  1 */   TABLE, 
/*  2 */   TABLE,
/*  3 */   TABLE,
/*  4 */   TABLE,
/*  5 */   TABLE,
/*  6 */   TABLE,
/*  7 */   TABLE,
/*  8 */   TABLE,
/*  9 */   TABLE,
/* 10 */   LOGS,
/* 11 */   LOGS,
/* 12 */   LOGS,
/* 13 */   LOGS,
/* 14 */   LOGS,
/* 15 */   LOGS,
/* 16 */   LOGS,
/* 17 */   LOGS,
/* 18 */   LOGS,
/* 19 */   LOGS,
/* 20 */   LOGS,
/* 21 */   LOGS,
/* 22 */   LOGS,
/* 23 */   SHIFT,
/* 24 */   SHIFT,
/* 25 */   SHIFT,
/* 26 */   SHIFT,
/* 27 */   SHIFT,
/* 28 */   SHIFT,
/* 29 */   SHIFT,
/* 30 */   SHIFT,
/* 31 */   SHIFT,
/* 32 */   SPLITW8 };

static int nw[33] = { 0, (1 << 1), (1 << 2), (1 << 3), (1 << 4), 
(1 << 5), (1 << 6), (1 << 7), (1 << 8), (1 << 9), (1 << 10),
(1 << 11), (1 << 12), (1 << 13), (1 << 14), (1 << 15), (1 << 16),
(1 << 17), (1 << 18), (1 << 19), (1 << 20), (1 << 21), (1 << 22),
(1 << 23), (1 << 24), (1 << 25), (1 << 26), (1 << 27), (1 << 28),
(1 << 29), (1 << 30), (1 << 31), -1 };

static int nwm1[33] = { 0, (1 << 1)-1, (1 << 2)-1, (1 << 3)-1, (1 << 4)-1, 
(1 << 5)-1, (1 << 6)-1, (1 << 7)-1, (1 << 8)-1, (1 << 9)-1, (1 << 10)-1,
(1 << 11)-1, (1 << 12)-1, (1 << 13)-1, (1 << 14)-1, (1 << 15)-1, (1 << 16)-1,
(1 << 17)-1, (1 << 18)-1, (1 << 19)-1, (1 << 20)-1, (1 << 21)-1, (1 << 22)-1,
(1 << 23)-1, (1 << 24)-1, (1 << 25)-1, (1 << 26)-1, (1 << 27)-1, (1 << 28)-1,
(1 << 29)-1, (1 << 30)-1, 0x7fffffff, 0xffffffff };
   
static int *galois_log_tables[33] = { NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL };

static int *galois_ilog_tables[33] = { NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL };

static int *galois_mult_tables[33] = { NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL };

static int *galois_div_tables[33] = { NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL };

/* Special case for w = 32 */

static int *galois_split_w8[7] = { NULL, NULL, NULL, NULL, NULL, NULL, NULL };

int galois_create_log_tables(int w)
{
  int j, b;

  if (w > 30) return -1;
  if (galois_log_tables[w] != NULL) return 0;
  galois_log_tables[w] = (int *) malloc(sizeof(int)*nw[w]);
  if (galois_log_tables[w] == NULL) return -1; 
  
  galois_ilog_tables[w] = (int *) malloc(sizeof(int)*nw[w]*3);
  if (galois_ilog_tables[w] == NULL) { 
    free(galois_log_tables[w]);
    galois_log_tables[w] = NULL;
    return -1;
  }
  
  for (j = 0; j < nw[w]; j++) {
    galois_log_tables[w][j] = nwm1[w];
    galois_ilog_tables[w][j] = 0;
  } 
  
  b = 1;
  for (j = 0; j < nwm1[w]; j++) {
    if (galois_log_tables[w][b] != nwm1[w]) {
      fprintf(stderr, "Galois_create_log_tables Error: j=%d, b=%d, B->J[b]=%d, J->B[j]=%d (0%o)\n",
              j, b, galois_log_tables[w][b], galois_ilog_tables[w][j], (b << 1) ^ prim_poly[w]);
      exit(1);
    }
    galois_log_tables[w][b] = j;
    galois_ilog_tables[w][j] = b;
    b = b << 1;
    if (b & nw[w]) b = (b ^ prim_poly[w]) & nwm1[w];
  }
  for (j = 0; j < nwm1[w]; j++) {
    galois_ilog_tables[w][j+nwm1[w]] = galois_ilog_tables[w][j];
    galois_ilog_tables[w][j+nwm1[w]*2] = galois_ilog_tables[w][j];
  } 
  galois_ilog_tables[w] += nwm1[w];
  return 0;
}

int galois_logtable_multiply(int x, int y, int w)
{
  int sum_j;

  if (x == 0 || y == 0) return 0;
  
  sum_j = galois_log_tables[w][x] + galois_log_tables[w][y];
  /* if (sum_j >= nwm1[w]) sum_j -= nwm1[w];    Don't need to do this, 
                                   because we replicate the ilog table twice.  */
  return galois_ilog_tables[w][sum_j];
}

int galois_logtable_divide(int x, int y, int w)
{
  int sum_j;
  int z;

  if (y == 0) return -1;
  if (x == 0) return 0; 
  sum_j = galois_log_tables[w][x] - galois_log_tables[w][y];
  /* if (sum_j < 0) sum_j += nwm1[w];   Don't need to do this, because we replicate the ilog table twice.   */
  z = galois_ilog_tables[w][sum_j];
  return z;
}

int galois_create_mult_tables(int w)
{
  int j, x, y, logx;

  if (w >= 14) return -1;

  if (galois_mult_tables[w] != NULL) return 0;
  galois_mult_tables[w] = (int *) malloc(sizeof(int) * nw[w] * nw[w]);
  if (galois_mult_tables[w] == NULL) return -1;
  
  galois_div_tables[w] = (int *) malloc(sizeof(int) * nw[w] * nw[w]);
  if (galois_div_tables[w] == NULL) {
    free(galois_mult_tables[w]);
    galois_mult_tables[w] = NULL;
    return -1;
  }
  if (galois_log_tables[w] == NULL) {
    if (galois_create_log_tables(w) < 0) {
      free(galois_mult_tables[w]);
      free(galois_div_tables[w]);
      galois_mult_tables[w] = NULL;
      galois_div_tables[w] = NULL;
      return -1;
    }
  }

 /* Set mult/div tables for x = 0 */
  j = 0;
  galois_mult_tables[w][j] = 0;   /* y = 0 */
  galois_div_tables[w][j] = -1;
  j++;
  for (y = 1; y < nw[w]; y++) {   /* y > 0 */
    galois_mult_tables[w][j] = 0;
    galois_div_tables[w][j] = 0;
    j++;
  }
  
  for (x = 1; x < nw[w]; x++) {  /* x > 0 */
    galois_mult_tables[w][j] = 0; /* y = 0 */
    galois_div_tables[w][j] = -1;
    j++;
    logx = galois_log_tables[w][x];
    for (y = 1; y < nw[w]; y++) {  /* y > 0 */
      galois_mult_tables[w][j] = galois_ilog_tables[w][logx+galois_log_tables[w][y]]; 
      galois_div_tables[w][j] = galois_ilog_tables[w][logx-galois_log_tables[w][y]]; 
      j++;
    }
  }
  return 0;
}

int galois_ilog(int value, int w)
{
  if (galois_ilog_tables[w] == NULL) {
    if (galois_create_log_tables(w) < 0) {
      fprintf(stderr, "Error: galois_ilog - w is too big.  Sorry\n");
      exit(1);
    }
  }
  return galois_ilog_tables[w][value];
}

int galois_log(int value, int w)
{
  if (galois_log_tables[w] == NULL) {
    if (galois_create_log_tables(w) < 0) {
      fprintf(stderr, "Error: galois_log - w is too big.  Sorry\n");
      exit(1);
    }
  }
  return galois_log_tables[w][value];
}


int galois_shift_multiply(int x, int y, int w)
{
  int prod;
  int i, j, ind;
  int k;
  int scratch[33];

  prod = 0;
  for (i = 0; i < w; i++) {
    scratch[i] = y;
    if (y & (1 << (w-1))) {
      y = y << 1;
      y = (y ^ prim_poly[w]) & nwm1[w];
    } else {
      y = y << 1;
    }
  }
  for (i = 0; i < w; i++) {
    ind = (1 << i);
    if (ind & x) {
      j = 1;
      for (k = 0; k < w; k++) {
        prod = prod ^ (j & scratch[i]);
        j = (j << 1);
      }
    }
  }
  return prod;
}

int galois_single_multiply(int x, int y, int w)
{
  int sum_j;
  int z;

  if (x == 0 || y == 0) return 0;
  
  if (mult_type[w] == TABLE) {
    if (galois_mult_tables[w] == NULL) {
      if (galois_create_mult_tables(w) < 0) {
        fprintf(stderr, "ERROR -- cannot make multiplication tables for w=%d\n", w);
        exit(1);
      }
    }
    return galois_mult_tables[w][(x<<w)|y];
  } else if (mult_type[w] == LOGS) {
    if (galois_log_tables[w] == NULL) {
      if (galois_create_log_tables(w) < 0) {
        fprintf(stderr, "ERROR -- cannot make log tables for w=%d\n", w);
        exit(1);
      }
    }
    sum_j = galois_log_tables[w][x] + galois_log_tables[w][y];
    z = galois_ilog_tables[w][sum_j];
    return z;
  } else if (mult_type[w] == SPLITW8) {
    if (galois_split_w8[0] == NULL) {
      if (galois_create_split_w8_tables() < 0) {
        fprintf(stderr, "ERROR -- cannot make log split_w8_tables for w=%d\n", w);
        exit(1);
      }
    }
    return galois_split_w8_multiply(x, y);
  } else if (mult_type[w] == SHIFT) {
    return galois_shift_multiply(x, y, w);
  }
  fprintf(stderr, "Galois_single_multiply - no implementation for w=%d\n", w);
  exit(1);
}

int galois_multtable_multiply(int x, int y, int w)
{
  return galois_mult_tables[w][(x<<w)|y];
}

int galois_single_divide(int a, int b, int w)
{
  int sum_j;

  if (mult_type[w] == TABLE) {
    if (galois_div_tables[w] == NULL) {
      if (galois_create_mult_tables(w) < 0) {
        fprintf(stderr, "ERROR -- cannot make multiplication tables for w=%d\n", w);
        exit(1);
      }
    }
    return galois_div_tables[w][(a<<w)|b];
  } else if (mult_type[w] == LOGS) {
    if (b == 0) return -1;
    if (a == 0) return 0;
    if (galois_log_tables[w] == NULL) {
      if (galois_create_log_tables(w) < 0) {
        fprintf(stderr, "ERROR -- cannot make log tables for w=%d\n", w);
        exit(1);
      }
    }
    sum_j = galois_log_tables[w][a] - galois_log_tables[w][b];
    return galois_ilog_tables[w][sum_j];
  } else {
    if (b == 0) return -1;
    if (a == 0) return 0;
    sum_j = galois_inverse(b, w);
    return galois_single_multiply(a, sum_j, w);
  }
  fprintf(stderr, "Galois_single_divide - no implementation for w=%d\n", w);
  exit(1);
}

int galois_shift_divide(int a, int b, int w)
{
  int inverse;

  if (b == 0) return -1;
  if (a == 0) return 0;
  inverse = galois_shift_inverse(b, w);
  return galois_shift_multiply(a, inverse, w);
}

int galois_multtable_divide(int x, int y, int w)
{
  return galois_div_tables[w][(x<<w)|y];
}

void galois_w08_region_multiply(char *region,      /* Region to multiply */
                                  int multby,       /* Number to multiply by */
                                  int nbytes,        /* Number of bytes in region */
                                  char *r2,          /* If r2 != NULL, products go here */
                                  int add)
{
  unsigned char *ur1, *ur2, *cp;
  unsigned char prod;
  int i, srow, j;
  unsigned long l, *lp2;
  unsigned char *lp;
  int sol;

  ur1 = (unsigned char *) region;
  ur2 = (r2 == NULL) ? ur1 : (unsigned char *) r2;

/* This is used to test its performance with respect to just calling galois_single_multiply 
  if (r2 == NULL || !add) {
    for (i = 0; i < nbytes; i++) ur2[i] = galois_single_multiply(ur1[i], multby, 8);
  } else {
    for (i = 0; i < nbytes; i++) {
      ur2[i] = (ur2[i]^galois_single_multiply(ur1[i], multby, 8));
    }
  }
 */

  if (galois_mult_tables[8] == NULL) {
    if (galois_create_mult_tables(8) < 0) {
      fprintf(stderr, "galois_08_region_multiply -- couldn't make multiplication tables\n");
      exit(1);
    }
  }
  srow = multby * nw[8];
  if (r2 == NULL || !add) {
    for (i = 0; i < nbytes; i++) {
      prod = galois_mult_tables[8][srow+ur1[i]];
      ur2[i] = prod;
    }
  } else {
    sol = sizeof(long);
    lp2 = &l;
    lp = (unsigned char *) lp2;
    for (i = 0; i < nbytes; i += sol) {
      cp = ur2+i;
      lp2 = (unsigned long *) cp;
      for (j = 0; j < sol; j++) {
        prod = galois_mult_tables[8][srow+ur1[i+j]];
        lp[j] = prod;
      }
      *lp2 = (*lp2) ^ l;
    }
  }
  return;
}

void galois_w16_region_multiply(char *region,      /* Region to multiply */
                                  int multby,       /* Number to multiply by */
                                  int nbytes,        /* Number of bytes in region */
                                  char *r2,          /* If r2 != NULL, products go here */
                                  int add)
{
  unsigned short *ur1, *ur2, *cp;
  int prod;
  int i, log1, j, log2;
  unsigned long l, *lp2, *lptop;
  unsigned short *lp;
  int sol;

  ur1 = (unsigned short *) region;
  ur2 = (r2 == NULL) ? ur1 : (unsigned short *) r2;
  nbytes /= 2;


/* This is used to test its performance with respect to just calling galois_single_multiply */
/*
  if (r2 == NULL || !add) {
    for (i = 0; i < nbytes; i++) ur2[i] = galois_single_multiply(ur1[i], multby, 16);
  } else {
    for (i = 0; i < nbytes; i++) {
      ur2[i] = (ur2[i]^galois_single_multiply(ur1[i], multby, 16));
    }
  }
  return;
  */

  if (multby == 0) {
    if (!add) {
      lp2 = (unsigned long *) ur2;
      ur2 += nbytes;
      lptop = (unsigned long *) ur2;
      while (lp2 < lptop) { *lp2 = 0; lp2++; }
    }
    return;
  }
    
  if (galois_log_tables[16] == NULL) {
    if (galois_create_log_tables(16) < 0) {
      fprintf(stderr, "galois_16_region_multiply -- couldn't make log tables\n");
      exit(1);
    }
  }
  log1 = galois_log_tables[16][multby];

  if (r2 == NULL || !add) {
    for (i = 0; i < nbytes; i++) {
      if (ur1[i] == 0) {
        ur2[i] = 0;
      } else {
        prod = galois_log_tables[16][ur1[i]] + log1;
        ur2[i] = galois_ilog_tables[16][prod];
      }
    }
  } else {
    sol = sizeof(long)/2;
    lp2 = &l;
    lp = (unsigned short *) lp2;
    for (i = 0; i < nbytes; i += sol) {
      cp = ur2+i;
      lp2 = (unsigned long *) cp;
      for (j = 0; j < sol; j++) {
        if (ur1[i+j] == 0) {
          lp[j] = 0;
        } else {
          log2 = galois_log_tables[16][ur1[i+j]];
          prod = log2 + log1;
          lp[j] = galois_ilog_tables[16][prod];
        }
      }
      *lp2 = (*lp2) ^ l;
    }
  }
  return; 
}

/* This will destroy mat, by the way */

void galois_invert_binary_matrix(int *mat, int *inv, int rows)
{
  int cols, i, j;
  int tmp;
 
  cols = rows;

  for (i = 0; i < rows; i++) inv[i] = (1 << i);

  /* First -- convert into upper triangular */

  for (i = 0; i < cols; i++) {

    /* Swap rows if we ave a zero i,i element.  If we can't swap, then the 
       matrix was not invertible */

    if ((mat[i] & (1 << i)) == 0) { 
      for (j = i+1; j < rows && (mat[j] & (1 << i)) == 0; j++) ;
      if (j == rows) {
        fprintf(stderr, "galois_invert_matrix: Matrix not invertible!!\n");
        exit(1);
      }
      tmp = mat[i]; mat[i] = mat[j]; mat[j] = tmp;
      tmp = inv[i]; inv[i] = inv[j]; inv[j] = tmp;
    }
 
    /* Now for each j>i, add A_ji*Ai to Aj */
    for (j = i+1; j != rows; j++) {
      if ((mat[j] & (1 << i)) != 0) {
        mat[j] ^= mat[i]; 
        inv[j] ^= inv[i];
      }
    }
  }

  /* Now the matrix is upper triangular.  Start at the top and multiply down */

  for (i = rows-1; i >= 0; i--) {
    for (j = 0; j < i; j++) {
      if (mat[j] & (1 << i)) {
/*        mat[j] ^= mat[i]; */
        inv[j] ^= inv[i];
      }
    }
  } 
}

int galois_inverse(int y, int w)
{

  if (y == 0) return -1;
  if (mult_type[w] == SHIFT || mult_type[w] == SPLITW8) return galois_shift_inverse(y, w);
  return galois_single_divide(1, y, w);
}

int galois_shift_inverse(int y, int w)
{
  int mat2[32];
  int inv2[32];
  int i;
 
  for (i = 0; i < w; i++) {
    mat2[i] = y;

    if (y & nw[w-1]) {
      y = y << 1;
      y = (y ^ prim_poly[w]) & nwm1[w];
    } else {
      y = y << 1;
    }
  }

  galois_invert_binary_matrix(mat2, inv2, w);

  return inv2[0]; 
}

int *galois_get_mult_table(int w)
{
  if (galois_mult_tables[w] == NULL) {
    if (galois_create_mult_tables(w)) {
      return NULL;
    }
  }
  return galois_mult_tables[w];
}

int *galois_get_div_table(int w) 
{
  if (galois_mult_tables[w] == NULL) {
    if (galois_create_mult_tables(w)) {
      return NULL;
    }
  }
  return galois_div_tables[w];
}

int *galois_get_log_table(int w)
{
  if (galois_log_tables[w] == NULL) {
    if (galois_create_log_tables(w)) {
      return NULL;
    }
  }
  return galois_log_tables[w];
}

int *galois_get_ilog_table(int w)
{
  if (galois_ilog_tables[w] == NULL) {
    if (galois_create_log_tables(w)) {
      return NULL;
    }
  }
  return galois_ilog_tables[w];
}

void galois_w32_region_multiply(char *region,      /* Region to multiply */
                                  int multby,       /* Number to multiply by */
                                  int nbytes,        /* Number of bytes in region */
                                  char *r2,          /* If r2 != NULL, products go here */
                                  int add)
{
  unsigned int *ur1, *ur2;
  int i, j, a, b, accumulator, i8, j8, k;
  int acache[4];

  ur1 = (unsigned int *) region;
  ur2 = (r2 == NULL) ? ur1 : (unsigned int *) r2;
  nbytes /= sizeof(int);

  if (galois_split_w8[0]== NULL) {
    if (galois_create_split_w8_tables() < 0) {
      fprintf(stderr, "galois_32_region_multiply -- couldn't make split multiplication tables\n");
      exit(1);
    }
  }

  /* If we're overwriting r2, then we can't do better than just calling split_multiply.
     We'll inline it here to save on the procedure call overhead */

  i8 = 0;
  for (i = 0; i < 4; i++) {
    acache[i] = (((multby >> i8) & 255) << 8);
    i8 += 8;
  }
  if (!add) {
    for (k = 0; k < nbytes; k++) {
      accumulator = 0;
      for (i = 0; i < 4; i++) {
        a = acache[i];
        j8 = 0;
        for (j = 0; j < 4; j++) {
          b = ((ur1[k] >> j8) & 255);
          accumulator ^= galois_split_w8[i+j][a|b];
          j8 += 8;
        }
      }
      ur2[k] = accumulator;
    }
  } else {
    for (k = 0; k < nbytes; k++) {
      accumulator = 0;
      for (i = 0; i < 4; i++) {
        a = acache[i];
        j8 = 0;
        for (j = 0; j < 4; j++) {
          b = ((ur1[k] >> j8) & 255);
          accumulator ^= galois_split_w8[i+j][a|b];
          j8 += 8;
        }
      }
      ur2[k] = (ur2[k] ^ accumulator);
    }
  }
  return;

}

void galois_region_xor(           char *r1,         /* Region 1 */
                                  char *r2,         /* Region 2 */
                                  char *r3,         /* Sum region (r3 = r1 ^ r2) -- can be r1 or r2 */
                                  int nbytes)       /* Number of bytes in region */
{
  long *l1;
  long *l2;
  long *l3;
  long *ltop;
  char *ctop;
  
  ctop = r1 + nbytes;
  ltop = (long *) ctop;
  l1 = (long *) r1;
  l2 = (long *) r2;
  l3 = (long *) r3;
 
  while (l1 < ltop) {
    *l3 = ((*l1)  ^ (*l2));
    l1++;
    l2++;
    l3++;
  }
}

int galois_create_split_w8_tables()
{
  int p1, p2, i, j, p1elt, p2elt, index, ishift, jshift, *table;

  if (galois_split_w8[0] != NULL) return 0;

  if (galois_create_mult_tables(8) < 0) return -1;

  for (i = 0; i < 7; i++) {
    galois_split_w8[i] = (int *) malloc(sizeof(int) * (1 << 16));
    if (galois_split_w8[i] == NULL) {
      for (i--; i >= 0; i--) free(galois_split_w8[i]);
      return -1;
    }
  }

  for (i = 0; i < 4; i += 3) {
    ishift = i * 8;
    for (j = ((i == 0) ? 0 : 1) ; j < 4; j++) {
      jshift = j * 8;
      table = galois_split_w8[i+j];
      index = 0;
      for (p1 = 0; p1 < 256; p1++) {
        p1elt = (p1 << ishift);
        for (p2 = 0; p2 < 256; p2++) {
          p2elt = (p2 << jshift);
          table[index] = galois_shift_multiply(p1elt, p2elt, 32);
          index++;
        }
      }
    }
  }
  return 0;
}

int galois_split_w8_multiply(int x, int y)
{
  int i, j, a, b, accumulator, i8, j8;

  accumulator = 0;
  
  i8 = 0;
  for (i = 0; i < 4; i++) {
    a = (((x >> i8) & 255) << 8);
    j8 = 0;
    for (j = 0; j < 4; j++) {
      b = ((y >> j8) & 255);
      accumulator ^= galois_split_w8[i+j][a|b];
      j8 += 8;
    }
    i8 += 8;
  }
  return accumulator;
}
