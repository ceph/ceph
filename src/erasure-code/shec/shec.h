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

#ifndef SHEC_H
#define SHEC_H

int *shec_reedsolomon_coding_matrix(int k, int m, int c, int w, int is_single);
int shec_make_decoding_matrix(bool prepare, int k, int m, int w, int *matrix,
    int *erased, int *avails, int *decoding_matrix, int *dm_ids, int *minimum);
int shec_matrix_decode(int k, int m, int w, int *matrix,
    int *erased, int *avails, char **data_ptrs, char **coding_ptrs, int size);

#endif
