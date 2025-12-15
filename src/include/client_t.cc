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

#include "client_t.h"
#include "common/Formatter.h"

#include <ostream>

void client_t::dump(ceph::Formatter *f) const {
  f->dump_int("id", v);
}

std::list<client_t> client_t::generate_test_instances() {
  std::list<client_t> ls;
  ls.emplace_back();
  ls.push_back(client_t(1));
  ls.push_back(client_t(123));
  return ls;
}

std::ostream& operator<<(std::ostream& out, const client_t& c) {
  return out << c.v;
}
