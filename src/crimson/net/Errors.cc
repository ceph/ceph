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

namespace crimson::net {

const std::error_category& net_category()
{
  struct category : public std::error_category {
    const char* name() const noexcept override {
      return "crimson::net";
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
        case error::corrupted_message:
          return "corrupted message";
        case error::protocol_aborted:
          return "protocol aborted";
        default:
          return "unknown";
      }
    }
  };
  static category instance;
  return instance;
}

} // namespace crimson::net
