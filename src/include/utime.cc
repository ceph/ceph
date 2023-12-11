// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "utime.h"
#include "common/Formatter.h"

void utime_t::dump(ceph::Formatter *f) const
{
  f->dump_int("seconds", tv.tv_sec);
  f->dump_int("nanoseconds", tv.tv_nsec);
}

void utime_t::generate_test_instances(std::list<utime_t*>& o)
{
  o.push_back(new utime_t());
  o.push_back(new utime_t());
  o.back()->tv.tv_sec = static_cast<__u32>((1L << 32) - 1);
  o.push_back(new utime_t());
  o.back()->tv.tv_nsec = static_cast<__u32>((1L << 32) - 1);
}
