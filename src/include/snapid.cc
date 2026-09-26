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

#include "snapid.h"

#include <fmt/compile.h>

#include <ostream>

std::ostream& operator<<(std::ostream& out, const snapid_t& s) {
  if (s == CEPH_NOSNAP)
    return out << "head";
  else if (s == CEPH_SNAPDIR)
    return out << "snapdir";
  else
    return out << std::hex << s.val << std::dec;
}

auto fmt::formatter<snapid_t>::format(const snapid_t& snp, format_context& ctx) const -> format_context::iterator
{
  if (snp == CEPH_NOSNAP) {
    return fmt::format_to(ctx.out(), "head");
  }
  if (snp == CEPH_SNAPDIR) {
    return fmt::format_to(ctx.out(), "snapdir");
  }
  return fmt::format_to(ctx.out(), FMT_COMPILE("{:x}"), snp.val);
}
