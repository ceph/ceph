// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "shard_id.h"
#include "common/Formatter.h"

const shard_id_t shard_id_t::NO_SHARD(-1);

void shard_id_t::dump(ceph::Formatter *f) const {
  f->dump_int("id", id);
}

std::list<shard_id_t> shard_id_t::generate_test_instances() {
  std::list<shard_id_t> ls;
  ls.push_back(shard_id_t(1));
  ls.push_back(shard_id_t(2));
  return ls;
}

std::ostream& operator<<(std::ostream& lhs, const shard_id_t& rhs)
{
  return lhs << (unsigned)(uint8_t)rhs.id;
}
