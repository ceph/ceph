// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */
#ifndef _CEPH_VARIANT_PRINT_H
#define _CEPH_VARIANT_PRINT_H

#include <ostream>
#include <variant>

template <typename T, typename... Ts>
std::ostream& operator<<(std::ostream& out, const std::variant<T, Ts...>& v) {
  std::visit([&out](const auto& value) {
    out << value;
  }, v);
  return out;
}

#endif
