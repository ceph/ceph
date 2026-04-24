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
#ifndef CEPH_SHA_DIGEST_FMT_H
#define CEPH_SHA_DIGEST_FMT_H

#include <ostream>

#include "include/sha_digest.h"

#include <fmt/core.h> // for FMT_VERSION
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

template<uint8_t S>
inline std::ostream &operator<<(std::ostream &out, const sha_digest_t<S> &b) {
  std::string str = b.to_str();
  return out << str;
}

#if FMT_VERSION >= 90000
template <uint8_t S> struct fmt::formatter<sha_digest_t<S>> : fmt::ostream_formatter {};
#endif

#endif
