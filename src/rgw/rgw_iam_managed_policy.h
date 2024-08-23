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

#include <optional>
#include <string>
#include <boost/container/flat_set.hpp>
#include "common/ceph_context.h"
#include "include/buffer_fwd.h"

namespace rgw::IAM {

struct Policy;

/// Return a managed policy by ARN.
auto get_managed_policy(CephContext* cct, std::string_view arn)
    -> std::optional<Policy>;

/// A serializable container for managed policy ARNs.
struct ManagedPolicies {
  boost::container::flat_set<std::string> arns;
};
void encode(const ManagedPolicies&, bufferlist&, uint64_t f=0);
void decode(ManagedPolicies&, bufferlist::const_iterator&);

} // namespace rgw::IAM
