// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */
#ifndef __CEPH_TYPES_H
#define __CEPH_TYPES_H

#include <include/types.h>

#ifndef UINT8_MAX
#define UINT8_MAX (255)
#endif

const shard_id_t shard_id_t::NO_SHARD(-1);

ostream &operator<<(ostream &lhs, const shard_id_t &rhs)
{
  return lhs << (unsigned)(uint8_t)rhs.id;
}

#endif
