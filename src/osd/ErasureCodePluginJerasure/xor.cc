// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 CERN/Switzerland
 *               
 *
 * Authors: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch> 
 *          
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include "vectorop.h"
#include <string.h>

void
vector_xor (vector_op_t* cw, vector_op_t* dw, int vector_words) {
  for (int i = 0; i < vector_words; ++i) {
    *cw++ ^= *dw++;
  }
}

void
vector_assign (vector_op_t* cw, vector_op_t* dw, int vector_words) {
  memcpy((void*)cw,(void*)dw,VECTOR_WORDSIZE * vector_words);
}
