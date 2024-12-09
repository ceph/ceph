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

#include "types.h"

#include <iomanip>

namespace {
inline std::ostream& format_u(std::ostream& out, const uint64_t v, const uint64_t n,
      const int index, const uint64_t mult, const char* u)
  {
    char buffer[32];

    if (index == 0) {
      (void) snprintf(buffer, sizeof(buffer), "%" PRId64 "%s", n, u);
    } else if ((v % mult) == 0) {
      // If this is an even multiple of the base, always display
      // without any decimal fraction.
      (void) snprintf(buffer, sizeof(buffer), "%" PRId64 "%s", n, u);
    } else {
      // We want to choose a precision that reflects the best choice
      // for fitting in 5 characters.  This can get rather tricky when
      // we have numbers that are very close to an order of magnitude.
      // For example, when displaying 10239 (which is really 9.999K),
      // we want only a single place of precision for 10.0K.  We could
      // develop some complex heuristics for this, but it's much
      // easier just to try each combination in turn.
      int i;
      for (i = 2; i >= 0; i--) {
        if (snprintf(buffer, sizeof(buffer), "%.*f%s", i,
          static_cast<double>(v) / mult, u) <= 7)
          break;
      }
    }

    return out << buffer;
  }
}

void client_t::dump(ceph::Formatter *f) const {
  f->dump_int("id", v);
}

void client_t::generate_test_instances(std::list<client_t*>& ls) {
  ls.push_back(new client_t);
  ls.push_back(new client_t(1));
  ls.push_back(new client_t(123));
}

std::ostream& operator<<(std::ostream& out, const client_t& c) {
  return out << c.v;
}

std::ostream& operator<<(std::ostream& out, const si_u_t& b)
{
  uint64_t n = b.v;
  int index = 0;
  uint64_t mult = 1;
  const char* u[] = {"", "k", "M", "G", "T", "P", "E"};

  while (n >= 1000 && index < 7) {
    n /= 1000;
    index++;
    mult *= 1000;
  }

  return format_u(out, b.v, n, index, mult, u[index]);
}

std::ostream& operator<<(std::ostream& out, const byte_u_t& b)
{
  uint64_t n = b.v;
  int index = 0;
  const char* u[] = {" B", " KiB", " MiB", " GiB", " TiB", " PiB", " EiB"};

  while (n >= 1024 && index < 7) {
    n /= 1024;
    index++;
  }

  return format_u(out, b.v, n, index, 1ULL << (10 * index), u[index]);
}

std::ostream& operator<<(std::ostream& out, const ceph_mon_subscribe_item& i)
{
  return out << (long)i.start
	     << ((i.flags & CEPH_SUBSCRIBE_ONETIME) ? "" : "+");
}

std::ostream& operator<<(std::ostream& out, const weightf_t& w)
{
  if (w.v < -0.01F) {
    return out << "-";
  } else if (w.v < 0.000001F) {
    return out << "0";
  } else {
    std::streamsize p = out.precision();
    return out << std::fixed << std::setprecision(5) << w.v << std::setprecision(p);
 }
}

void shard_id_t::dump(ceph::Formatter *f) const {
  f->dump_int("id", id);
}

void shard_id_t::generate_test_instances(std::list<shard_id_t*>& ls) {
  ls.push_back(new shard_id_t(1));
  ls.push_back(new shard_id_t(2));
}

void errorcode32_t::dump(ceph::Formatter *f) const {
  f->dump_int("code", code);
}

void errorcode32_t::generate_test_instances(std::list<errorcode32_t*>& ls) {
  ls.push_back(new errorcode32_t(1));
  ls.push_back(new errorcode32_t(2));
}
