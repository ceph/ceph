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

#include <boost/intrusive/set.hpp>

#include <h3/h3.h>
#include "connection.h"

namespace rgw::h3 {

struct cid_compare {
  bool operator()(const connection_id& lhs, const connection_id& rhs) const {
    return std::lexicographical_compare(lhs.begin(), lhs.end(),
                                        rhs.begin(), rhs.end());
  }
  bool operator()(const ConnectionImpl& lhs, const ConnectionImpl& rhs) const {
    return (*this)(lhs.cid, rhs.cid);
  }
};

struct cid_key {
  using type = const connection_id&;
  type operator()(const ConnectionImpl& c) { return c.cid; }
};

/// A set of connections sorted by id.
using connection_set = boost::intrusive::set<ConnectionImpl,
      boost::intrusive::compare<cid_compare>,
      boost::intrusive::key_of_value<cid_key>>;

} // namespace rgw::h3
