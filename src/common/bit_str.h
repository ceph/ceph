// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_COMMON_BIT_STR_H
#define CEPH_COMMON_BIT_STR_H

#include <cstdint>
#include <iosfwd>
#include <functional>

namespace ceph {
  class Formatter;
}

extern void print_bit_str(
    uint64_t bits,
    std::ostream &out,
    const std::function<const char*(uint64_t)> &func,
    bool dump_bit_val = false);

extern void dump_bit_str(
    uint64_t bits,
    ceph::Formatter *f,
    const std::function<const char*(uint64_t)> &func,
    bool dump_bit_val = false);

#endif /* CEPH_COMMON_BIT_STR_H */
