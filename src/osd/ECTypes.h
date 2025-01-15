// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

struct ec_align_t {
  uint64_t offset;
  uint64_t size;
  uint32_t flags;
  friend std::ostream &operator<<(std::ostream &lhs, const ec_align_t &rhs) {
    return lhs << rhs.offset << ","
               << rhs.size << ","
               << rhs.flags;
  }
};

