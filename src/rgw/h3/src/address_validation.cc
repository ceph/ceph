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

#include <algorithm>
#include <exception>
#include <iterator>
#include <string_view>

#include "address_validation.h"

namespace rgw::h3 {

static constexpr auto token_prefix = std::string_view{"RGW"};

// TODO: is this sufficiently difficult to guess?
// see 8.1.4. Address Validation Token Integrity
// https://www.rfc-editor.org/rfc/rfc9000.html#name-address-validation-token-in
// this algorithm came from https://github.com/cloudflare/quiche/blob/master/quiche/examples/http3-server.c#L127-L163
size_t write_token(const connection_id& dcid,
                   const ip::udp::endpoint& peer,
                   address_validation_token& token)
{
  const size_t bytes = token_prefix.size() + peer.size() + dcid.size();
  if (bytes > token.max_size()) {
    throw std::length_error{"token max size exceeded"};
  }
  auto out = std::back_inserter(token);
  std::copy(token_prefix.begin(), token_prefix.end(), out);
  std::copy_n(reinterpret_cast<const uint8_t*>(peer.data()), peer.size(), out);
  std::copy(dcid.begin(), dcid.end(), out);
  return bytes;
}

size_t validate_token(const address_validation_token& token,
                      const ip::udp::endpoint& peer,
                      connection_id& dcid)
{
  if (token.size() <= token_prefix.size() + peer.size()) {
    return 0;
  }
  auto p = std::mismatch(token_prefix.begin(), token_prefix.end(),
                         token.begin());
  if (p.first != token_prefix.end()) {
    return 0;
  }
  const auto peer_begin = reinterpret_cast<const uint8_t*>(peer.data());
  const auto peer_end = peer_begin + peer.size();
  auto q = std::mismatch(peer_begin, peer_end, p.second);
  if (q.first != peer_end) {
    return 0;
  }
  const size_t bytes = std::distance(q.second, token.end());
  if (bytes > dcid.max_size()) {
    return 0;
  }
  dcid.resize(bytes, boost::container::default_init);
  std::copy(q.second, token.end(), dcid.begin());
  return bytes;
}

} // namespace rgw::h3
