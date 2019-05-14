// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "Errors.h"

namespace ceph::net {

const std::error_category& net_category()
{
  struct category : public std::error_category {
    const char* name() const noexcept override {
      return "ceph::net";
    }

    std::string message(int ev) const override {
      switch (static_cast<error>(ev)) {
        case error::success:
          return "success";
        case error::bad_connect_banner:
          return "bad connect banner";
        case error::bad_peer_address:
          return "bad peer address";
        case error::negotiation_failure:
          return "negotiation failure";
        case error::read_eof:
          return "read eof";
        case error::connection_aborted:
          return "connection aborted";
        case error::connection_refused:
          return "connection refused";
        case error::connection_reset:
          return "connection reset";
        case error::corrupted_message:
          return "corrupted message";
        default:
          return "unknown";
      }
    }

    // unfortunately, seastar throws connection errors with the system category,
    // rather than the generic category that would match their counterparts
    // in std::errc. we add our own errors for them, so we can match either
    std::error_condition default_error_condition(int ev) const noexcept override {
      switch (static_cast<error>(ev)) {
        case error::connection_aborted:
          return std::errc::connection_aborted;
        case error::connection_refused:
          return std::errc::connection_refused;
        case error::connection_reset:
          return std::errc::connection_reset;
        default:
          return std::error_condition(ev, *this);
      }
    }

    bool equivalent(int code, const std::error_condition& cond) const noexcept override {
      if (error_category::equivalent(code, cond)) {
        return true;
      }
      switch (static_cast<error>(code)) {
        case error::connection_aborted:
          return cond == std::errc::connection_aborted
              || cond == std::error_condition(ECONNABORTED, std::system_category());
        case error::connection_refused:
          return cond == std::errc::connection_refused
              || cond == std::error_condition(ECONNREFUSED, std::system_category());
        case error::connection_reset:
          return cond == std::errc::connection_reset
              || cond == std::error_condition(ECONNRESET, std::system_category());
        default:
          return false;
      }
    }

    bool equivalent(const std::error_code& code, int cond) const noexcept override {
      if (error_category::equivalent(code, cond)) {
        return true;
      }
      switch (static_cast<error>(cond)) {
        case error::connection_aborted:
          return code == std::errc::connection_aborted
              || code == std::error_code(ECONNABORTED, std::system_category());
        case error::connection_refused:
          return code == std::errc::connection_refused
              || code == std::error_code(ECONNREFUSED, std::system_category());
        case error::connection_reset:
          return code == std::errc::connection_reset
              || code == std::error_code(ECONNRESET, std::system_category());
        default:
          return false;
      }
    }
  };
  static category instance;
  return instance;
}

} // namespace ceph::net
