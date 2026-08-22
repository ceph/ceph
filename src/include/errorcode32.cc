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

#include "errorcode32.h"
#include "common/Formatter.h"

void errorcode32_t::dump(ceph::Formatter *f) const {
  f->dump_int("code", code);
}

std::list<errorcode32_t> errorcode32_t::generate_test_instances() {
  std::list<errorcode32_t> ls;
  ls.push_back(errorcode32_t(1));
  ls.push_back(errorcode32_t(2));
  return ls;
}
