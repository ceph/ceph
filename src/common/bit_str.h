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

#include <ostream>
#include <functional>
#include <stdint.h>

namespace ceph {
  class Formatter;
}

extern void print_bit_str(
    uint64_t bits,
    std::ostream &out,
    std::function<const char*(uint64_t)> func);

extern void dump_bit_str(
    uint64_t bits,
    ceph::Formatter *f,
    std::function<const char*(uint64_t)> func);

#endif /* CEPH_COMMON_BIT_STR_H */
