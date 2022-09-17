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

#include <h3/types.h>

namespace rgw::h3 {

/// Generate an address validation token to be sent in a stateless retry packet.
/// Throws std::length_error if it would exceed token.max_size().
size_t write_token(const connection_id& dcid,
                   const ip::udp::endpoint& peer,
                   address_validation_token& token);

/// Validate the token of an incoming packet. It must come from the same address
/// that we sent the stateless retry packet to. Returns 0 on error, or the
/// number of bytes copied into dcid.
size_t validate_token(const address_validation_token& token,
                      const ip::udp::endpoint& peer,
                      connection_id& dcid);

} // namespace rgw::h3
