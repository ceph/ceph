// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>

class DoutPrefixProvider;
namespace rgw::sal { class RadosStore; }

// the squid release changes the format of topic/notification metadata. once the
// notification_v2 feature gets enabled, this migration logic runs on startup to
// convert all v1 metadata to the v2 format
namespace rgwrados::topic_migration {

int migrate(const DoutPrefixProvider* dpp,
            rgw::sal::RadosStore* driver,
            boost::asio::io_context& context,
            boost::asio::yield_context yield);

} // rgwrados::topic_migration
