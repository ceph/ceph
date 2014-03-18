/* *
 * Copyright (c) 2014, James S. Plank and Kevin Greenan
 * All rights reserved.
 *
 * Jerasure - A C/C++ Library for a Variety of Reed-Solomon and RAID-6 Erasure
 * Coding Techniques
 *
 * Revision 2.0: Galois Field backend now links to GF-Complete
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *  - Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 *  - Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 *  - Neither the name of the University of Tennessee nor the names of its
 *    contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* Jerasure's authors:

   Revision 2.x - 2014: James S. Plank and Kevin M. Greenan
   Revision 1.2 - 2008: James S. Plank, Scott Simmerman and Catherine D. Schuman.
   Revision 1.0 - 2007: James S. Plank
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "galois.h"
#include "jerasure.h"
#include "liberation.h"

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

int *liberation_coding_bitmatrix(int k, int w)
{
  int *matrix, i, j, index;

  if (k > w) return NULL;
  matrix = talloc(int, 2*k*w*w);
  if (matrix == NULL) return NULL;
  bzero(matrix, sizeof(int)*2*k*w*w);
  
  /* Set up identity matrices */

  for(i = 0; i < w; i++) {
    index = i*k*w+i;
    for (j = 0; j < k; j++) {
      matrix[index] = 1;
      index += w;
    }
  }

  /* Set up liberation matrices */

  for (j = 0; j < k; j++) {
    index = k*w*w+j*w;
    for (i = 0; i < w; i++) {
      matrix[index+(j+i)%w] = 1;
      index += (k*w);
    }
    if (j > 0) {
      i = (j*((w-1)/2))%w;
      matrix[k*w*w+j*w+i*k*w+(i+j-1)%w] = 1;
    }
  }
  return matrix;
}
  

int *liber8tion_coding_bitmatrix(int k)
{
  int *matrix, i, j, index;
  int w;

  w = 8;
  if (k > w) return NULL;
  matrix = talloc(int, 2*k*w*w);
  if (matrix == NULL) return NULL;
  bzero(matrix, sizeof(int)*2*k*w*w);
  
  /* Set up identity matrices */

  for(i = 0; i < w; i++) {
    index = i*k*w+i;
    for (j = 0; j < k; j++) {
      matrix[index] = 1;
      index += w;
    }
  }

  /* Set up liber8tion matrices */

  index = k*w*w;

  if (k == 0) return matrix;
  matrix[index+0*k*w+0*w+0] = 1;
  matrix[index+1*k*w+0*w+1] = 1;
  matrix[index+2*k*w+0*w+2] = 1;
  matrix[index+3*k*w+0*w+3] = 1;
  matrix[index+4*k*w+0*w+4] = 1;
  matrix[index+5*k*w+0*w+5] = 1;
  matrix[index+6*k*w+0*w+6] = 1;
  matrix[index+7*k*w+0*w+7] = 1;

  if (k == 1) return matrix;
  matrix[index+0*k*w+1*w+7] = 1;
  matrix[index+1*k*w+1*w+3] = 1;
  matrix[index+2*k*w+1*w+0] = 1;
  matrix[index+3*k*w+1*w+2] = 1;
  matrix[index+4*k*w+1*w+6] = 1;
  matrix[index+5*k*w+1*w+1] = 1;
  matrix[index+6*k*w+1*w+5] = 1;
  matrix[index+7*k*w+1*w+4] = 1;
  matrix[index+4*k*w+1*w+7] = 1;

  if (k == 2) return matrix;
  matrix[index+0*k*w+2*w+6] = 1;
  matrix[index+1*k*w+2*w+2] = 1;
  matrix[index+2*k*w+2*w+4] = 1;
  matrix[index+3*k*w+2*w+0] = 1;
  matrix[index+4*k*w+2*w+7] = 1;
  matrix[index+5*k*w+2*w+3] = 1;
  matrix[index+6*k*w+2*w+1] = 1;
  matrix[index+7*k*w+2*w+5] = 1;
  matrix[index+1*k*w+2*w+3] = 1;

  if (k == 3) return matrix;
  matrix[index+0*k*w+3*w+2] = 1;
  matrix[index+1*k*w+3*w+5] = 1;
  matrix[index+2*k*w+3*w+7] = 1;
  matrix[index+3*k*w+3*w+6] = 1;
  matrix[index+4*k*w+3*w+0] = 1;
  matrix[index+5*k*w+3*w+3] = 1;
  matrix[index+6*k*w+3*w+4] = 1;
  matrix[index+7*k*w+3*w+1] = 1;
  matrix[index+5*k*w+3*w+4] = 1;

  if (k == 4) return matrix;
  matrix[index+0*k*w+4*w+5] = 1;
  matrix[index+1*k*w+4*w+6] = 1;
  matrix[index+2*k*w+4*w+1] = 1;
  matrix[index+3*k*w+4*w+7] = 1;
  matrix[index+4*k*w+4*w+2] = 1;
  matrix[index+5*k*w+4*w+4] = 1;
  matrix[index+6*k*w+4*w+3] = 1;
  matrix[index+7*k*w+4*w+0] = 1;
  matrix[index+2*k*w+4*w+0] = 1;

  if (k == 5) return matrix;
  matrix[index+0*k*w+5*w+1] = 1;
  matrix[index+1*k*w+5*w+2] = 1;
  matrix[index+2*k*w+5*w+3] = 1;
  matrix[index+3*k*w+5*w+4] = 1;
  matrix[index+4*k*w+5*w+5] = 1;
  matrix[index+5*k*w+5*w+6] = 1;
  matrix[index+6*k*w+5*w+7] = 1;
  matrix[index+7*k*w+5*w+0] = 1;
  matrix[index+7*k*w+5*w+2] = 1;

  if (k == 6) return matrix;
  matrix[index+0*k*w+6*w+3] = 1;
  matrix[index+1*k*w+6*w+0] = 1;
  matrix[index+2*k*w+6*w+6] = 1;
  matrix[index+3*k*w+6*w+5] = 1;
  matrix[index+4*k*w+6*w+1] = 1;
  matrix[index+5*k*w+6*w+7] = 1;
  matrix[index+6*k*w+6*w+4] = 1;
  matrix[index+7*k*w+6*w+2] = 1;
  matrix[index+6*k*w+6*w+5] = 1;

  if (k == 7) return matrix;
  matrix[index+0*k*w+7*w+4] = 1;
  matrix[index+1*k*w+7*w+7] = 1;
  matrix[index+2*k*w+7*w+1] = 1;
  matrix[index+3*k*w+7*w+5] = 1;
  matrix[index+4*k*w+7*w+3] = 1;
  matrix[index+5*k*w+7*w+2] = 1;
  matrix[index+6*k*w+7*w+0] = 1;
  matrix[index+7*k*w+7*w+6] = 1;
  matrix[index+3*k*w+7*w+1] = 1;

  return matrix;
}
  
int *blaum_roth_coding_bitmatrix(int k, int w)
{
  int *matrix, i, j, index, l, m, p;

  if (k > w) return NULL ;

  matrix = talloc(int, 2*k*w*w);
  if (matrix == NULL) return NULL;
  bzero(matrix, sizeof(int)*2*k*w*w);
  
  /* Set up identity matrices */

  for(i = 0; i < w; i++) {
    index = i*k*w+i;
    for (j = 0; j < k; j++) {
      matrix[index] = 1;
      index += w;
    }
  }

  /* Set up blaum_roth matrices -- Ignore identity */

  p = w+1;
  for (j = 0; j < k; j++) {
    index = k*w*w+j*w;
    if (j == 0) {
      for (l = 0; l < w; l++) {
        matrix[index+l] = 1;
        index += k*w;
      }
    } else {
      i = j;
      for (l = 1; l <= w; l++) {
        if (l != p-i) {
          m = l+i;
          if (m >= p) m -= p;
          m--;
          matrix[index+m] = 1;
        } else {
          matrix[index+i-1] = 1;
          if (i%2 == 0) {
            m = i/2;
          } else {
            m = (p/2) + 1 + (i/2);
          }
          m--;
          matrix[index+m] = 1;
        }
        index += k*w;
      }
    }
  }

  return matrix;
}
