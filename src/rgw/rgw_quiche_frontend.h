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

#include <memory>
#include <boost/asio/io_context.hpp>

namespace ceph::common { class CephContext; }
class RGWProcessEnv;
class RGWFrontendConfig;
class RGWFrontend;


/// HTTP/3 frontend
namespace rgw::h3 {

// h3 frontend factory
auto create_frontend(ceph::common::CephContext* cct,
                     const RGWProcessEnv& env,
                     const RGWFrontendConfig* conf,
                     boost::asio::io_context& context)
  -> std::unique_ptr<RGWFrontend>;

} // namespace rgw::h3
