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

#include "common/async/yield_context.h"
#include <h3/h3.h>
#include "rgw_client_io.h"

namespace rgw::h3 {

/// Implements the RestfulClient abstraction that radosgw uses to read http
/// requests and write responses.
class ClientIO : public io::RestfulClient {
  boost::asio::yield_context yield;
  Connection* conn;
  StreamIO stream;
  http::fields request;
  http::fields response;
  ip::udp::endpoint local_endpoint;
  ip::udp::endpoint remote_endpoint;

  RGWEnv env;

 public:
  ClientIO(asio::io_context& context, boost::asio::yield_context yield,
           Connection* conn, uint64_t stream_id, http::fields request,
           ip::udp::endpoint local_endpoint,
           ip::udp::endpoint remote_endpoint);

  int init_env(CephContext* cct) override;
  void flush() override {}

  size_t send_status(int status, const char* status_name) override;
  size_t send_100_continue() override;
  size_t send_header(const std::string_view& name,
                     const std::string_view& value) override;
  size_t send_content_length(uint64_t len) override;
  size_t complete_header() override;

  size_t recv_body(char* buf, size_t len) override;
  size_t send_body(const char* buf, size_t len) override;
  size_t complete_request() override;

  RGWEnv& get_env() noexcept override {
    return env;
  }
};

} // namespace rgw::h3
