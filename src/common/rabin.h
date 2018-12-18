// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Yuan-Ting Hsieh, Hsuan-Heng Wu
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_RABIN_H_
#define CEPH_COMMON_RABIN_H_

#define WINDOW_SIZE 48
#define RABIN_PRIME 3
#define RABIN_MASK ((1<<5) -1)
#define MOD_PRIME 6148914691236517051
#define POW_47 907234050803559263

/*
 * Given a pointer to data (start of data) and offset
 * returns a Rabin-fingerprint
 */
uint64_t gen_rabin_hash(char* chunk_data, uint64_t off);

/*
 * Given a Rabin-fingerprint, determines if it is
 * end of chunk
 */
bool end_of_chunk(const uint64_t fp);

bool end_of_chunk(const uint64_t fp, int numbits);

/*
 * Given a bufferlist of inputdata, use Rabin-fingerprint to
 * chunk it and return the chunked result
 *
 */
void get_rabin_chunks(
                  size_t min,
                  size_t max,
                  bufferlist& inputdata,
                  vector<bufferlist> * output_chunks);

void get_rabin_chunks(
                  size_t min,
                  size_t max,
                  bufferlist& inputdata,
                  vector<bufferlist> * output_chunks, int numbits);


#endif // CEPH_COMMON_RABIN_H_
