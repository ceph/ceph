// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/neorados/RADOS.hpp"

constexpr int to_create = 10'000'000;

int main() {
  for (int i = 0; i < to_create; ++i) {
    neorados::ReadOp op;
    bufferlist bl;
    std::uint64_t sz;
    ceph::real_time tm;
    boost::container::flat_map<std::string, ceph::buffer::list> xattrs;
    boost::container::flat_map<std::string, ceph::buffer::list> omap;
    bool trunc;
    op.read(0, 0, &bl);
    op.stat(&sz, &tm);
    op.get_xattrs(&xattrs);
    op.get_omap_vals(std::nullopt, std::nullopt, 1000, &omap, &trunc);
  }
}
