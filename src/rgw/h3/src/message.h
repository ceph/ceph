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

#include <boost/asio/ip/udp.hpp>
#include <boost/container/static_vector.hpp>
#include <boost/intrusive/list.hpp>

namespace rgw::h3 {

static constexpr size_t max_datagram_size = 4096;

using message_buffer = boost::container::static_vector<
    uint8_t, max_datagram_size>;

struct message : boost::intrusive::list_base_hook<> {
  message_buffer buffer{message_buffer::max_size(),
                        boost::container::default_init};
  boost::asio::ip::udp::endpoint peer; // remote address
};

} // namespace rgw::h3
