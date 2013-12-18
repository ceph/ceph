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
#ifndef CEPH_XOR_H
#define	CEPH_XOR_H

#include "vectorop.h"
#include <set>

/**
 * @brief XOR a vector source block into a target block e.g. target^=source 
 * @param target is the start addr of the target block aligned to a vector_word
 * @param source is the start addr of the source block aligned to a vector_word
 * @param vector_words are the number of vector_op_t words
 */
void vector_xor (vector_op_t* target, vector_op_t* source, int vector_words);

/**
 * @brief assign a vector source block to a target block e.g. target=source 
 * @param target is the start addr of the target block aligned to a vector_word
 * @param source is the start addr of the source block aligned to a vector_word
 * @param vector_words are the number of vector_op_t words
 */
void vector_assign (vector_op_t*, vector_op_t*, int vector_words);

void vector_sse_xor(const std::set<vector_op_t*> &data, vector_op_t* parity, unsigned size);
#endif	/* CEPH_XOR_H */

