// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#include "sobject.h"
#include "common/Formatter.h"

#include <cstdio>

void sobject_t::dump(ceph::Formatter *f) const {
  f->dump_stream("oid") << oid;
  f->dump_stream("snap") << snap;
}

std::list<sobject_t> sobject_t::generate_test_instances() {
  std::list<sobject_t> o;
  o.emplace_back();
  o.push_back(sobject_t{object_t("myobject"), 123});
  return o;
}

std::ostream& operator<<(std::ostream& out, const sobject_t &o) {
  return out << o.oid << "/" << o.snap;
}
