// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <concepts>
#include <iosfwd>

template<typename T>
concept HasPrint = requires(T t, std::ostream& u) {
  { t.print(u) } -> std::same_as<void>;
};

template<typename T> requires HasPrint<T>
static inline std::ostream& operator<<(std::ostream& out, T&& t)
{
  t.print(out);
  return out;
}
