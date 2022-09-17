// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <algorithm>
#include <iterator>
#include <ostream>
#include <boost/container/static_vector.hpp>

namespace boost::container {

/// Print the opaque string as hexidecimal digits.
template <std::size_t N>
inline std::ostream& operator<<(std::ostream& out,
                                const static_vector<uint8_t, N>& rhs)
{
  out << std::hex;
  std::copy(rhs.begin(), rhs.end(), std::ostream_iterator<int>{out});
  return out << std::dec;
}

} // namespace boost::container

namespace rgw::h3 {

struct PacketType {
  uint8_t type;
};

inline std::ostream& operator<<(std::ostream& out, const PacketType& rhs)
{
  switch (rhs.type) {
    case 1: return out << "initial";
    case 2: return out << "retry";
    case 3: return out << "handshake";
    case 4: return out << "0rtt";
    case 5: return out << "short";
    case 6: return out << "version";
    default: return out << "unknown:" << static_cast<int>(rhs.type);
  }
}

} // namespace rgw::h3
